import * as k8s from '@kubernetes/client-node';
import * as cmd from 'cmd-ts';
import * as uuid from 'uuid';
const readline: any = require('readline-promise').default;
import '@colors/colors';

async function getPodsMountingPVC(kc: k8s.KubeConfig, pvcName: string, namespaceName: string)
{
    // Ported from https://github.com/kubernetes/kubernetes/pull/65837/commits/d956994857e8e47cbb21ac4765c2db2562640364.

    let result: k8s.V1Pod[] = [];

    let coreAPI = kc.makeApiClient(k8s.CoreV1Api);
    
    let pods = await coreAPI.listNamespacedPod(namespaceName);

    for (let pod of pods.body.items)
    {
        for (let volume of pod.spec?.volumes || [])
        {
            if (volume.persistentVolumeClaim && volume.persistentVolumeClaim.claimName == pvcName)
            {
                result.push(pod);
            }
        }
    }

    return result;
}

async function getPVForPVC(kc: k8s.KubeConfig, pvc: k8s.V1PersistentVolumeClaim)
{
    let coreAPI = kc.makeApiClient(k8s.CoreV1Api);

    let pvs = await coreAPI.listPersistentVolume();

    for (let pv of pvs.body.items)
    {
        if (pv.status?.phase == 'Bound')
        {
            let claimRef = pv.spec?.claimRef;

            if (claimRef && claimRef.uid == pvc.metadata?.uid)
            {
                return pv;
            }
        }
    }

    return null;
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

function output(str: string)
{
    process.stdout.write(str);
}

async function input(prompt: string)
{
    return await rl.questionAsync(prompt);
}

async function main()
{
    const app = cmd.command({
        name: 'kubernetes-change-storage-class',
        args: {
            context: cmd.option({ type: cmd.string, long: 'context', description: 'The name of the kubeconfig context to use.' }),
            namespaceName: cmd.option({ type: cmd.string, short: 'n', long: 'namespace', description: 'The namespace containing the PVC to convert.' }),
            newStorageClassName: cmd.option({ type: cmd.string, long: 'to-storage-class', description: 'The name of the new storage class for the PVC.' }),
            pvcName: cmd.positional({ type: cmd.string, displayName: 'pvc', description: 'The name of the PVC to convert.' }),
        },
        handler: async ({ context, namespaceName, newStorageClassName, pvcName }) => {
            const kc = new k8s.KubeConfig();
            kc.loadFromDefault();
            kc.setCurrentContext(context);

            let coreAPI = kc.makeApiClient(k8s.CoreV1Api);
            let batchAPI = kc.makeApiClient(k8s.BatchV1Api);

            output(`Gathering info...`);

            let pvc = (await coreAPI.readNamespacedPersistentVolumeClaim(pvcName, namespaceName)).body;
            let pv = await getPVForPVC(kc, pvc);
            let pods = await getPodsMountingPVC(kc, pvcName, namespaceName);

            output(`Done!\n`);
            output(`\n`);

            const runID: string = uuid.v4();

            if (pv)
            {
                output(`${`Here's what we've found:`.underline}\n`);
                output(`    PVC ${pvc.metadata?.name?.blue} is currently bound to PV ${pv.metadata?.name?.blue}, and mounted by pods [${pods.map(p => p.metadata?.name?.blue).join(',')}].\n`);
                output(`\n`);

                let steps = [
                    { description: `Change the PV's reclaim policy (${'.spec.persistentVolumeReclaimPolicy'.green}) to ${'Retain'.red}. This means the PV will not be deleted when the PVC is deleted.`, run: async () => {
                        await coreAPI.patchPersistentVolume(pv!.metadata?.name!, [
                            {
                                op: "replace",
                                path: "/spec/persistentVolumeReclaimPolicy",
                                value: "Retain"
                            }
                        ], undefined, undefined, undefined, undefined, {
                            headers: {
                                'Content-type': k8s.PatchUtils.PATCH_FORMAT_JSON_PATCH
                            }
                        });

                        return true;
                    } },
                    { description: `Delete the PVC. You will need to get rid of any Pods that mount the PVC (i.e. scale Deployments/StatefulSets/ReplicaSets to 0) before the deletion can complete.`, run: async () => {
                        await coreAPI.deleteNamespacedPersistentVolumeClaim(pvc.metadata?.name!, pvc.metadata?.namespace!);

                        while (true)
                        {
                            pods = await getPodsMountingPVC(kc, pvcName, namespaceName);

                            process.stdout.clearLine(0);
                            process.stdout.cursorTo(0);
                            output(`    Waiting for the deletion to complete. Pods still using PVC: [${pods.map(p => p.metadata?.name?.blue).join(',')}]`);

                            try
                            {
                                await coreAPI.readNamespacedPersistentVolumeClaim(pvc.metadata?.name!, pvc.metadata?.namespace!);
                            }
                            catch (e: any)
                            {
                                if (e instanceof k8s.HttpError && e.statusCode == 404)
                                {
                                    break;
                                }
                            }

                            await new Promise((resolve, reject) => setTimeout(resolve, 1000));
                        }

                        output(`\n`);

                        return true;
                    } },
                    { description: `Create a new PVC, identical to the old one, except for the storage class, which will be changed to ${newStorageClassName.blue}. This will cause a new PV to be provisioned by the ${newStorageClassName.blue} storage provider.`, run: async () => {
                        await coreAPI.createNamespacedPersistentVolumeClaim(pvc.metadata?.namespace!, {
                            ...pvc,
                            metadata: {
                                namespace: pvc.metadata?.namespace,
                                name: pvc.metadata?.name,
                                labels: pvc.metadata?.labels,
                                annotations: Object.fromEntries(Object.entries(pvc.metadata?.annotations!).filter(([k, v]) => !k.startsWith('pv.kubernetes.io/')))
                            },
                            status: undefined,
                            spec: {
                                ...pvc.spec,
                                storageClassName: newStorageClassName,
                                volumeName: undefined
                            }
                        });

                        return true;
                    } },
                    { description: `Create a temporary PVC to allow us to mount the old PV in order to copy the data across to the new one.`, run: async () => {
                        await coreAPI.patchPersistentVolume(pv!.metadata?.name!, [
                            {
                                op: "remove",
                                path: "/spec/claimRef"
                            },
                        ], undefined, undefined, undefined, undefined, {
                            headers: {
                                'Content-type': k8s.PatchUtils.PATCH_FORMAT_JSON_PATCH
                            }
                        });

                        await coreAPI.createNamespacedPersistentVolumeClaim(pvc.metadata?.namespace!, {
                            metadata: {
                                name: `csc-${runID}-old-data`
                            },
                            spec: {
                                storageClassName: pvc!.spec?.storageClassName!,
                                accessModes: pvc!.spec?.accessModes!,
                                resources: pvc!.spec?.resources!,
                                volumeName: pv!.metadata?.name!
                            }
                        });

                        return true;
                    } },
                    { description: `Create a Job to copy the data. This Job's Pod will mount both the old PV and the new one.`, run: async () => {
                        await batchAPI.createNamespacedJob(pvc!.metadata?.namespace!, {
                            metadata: {
                                name: `csc-${runID}-copy-data`
                            },
                            spec: {
                                backoffLimit: 0,
                                template: {
                                    spec: {
                                        restartPolicy: "Never",
                                        containers: [
                                            {
                                                name: "rsync",
                                                image: "eeacms/rsync",
                                                args: ["rsync", "-avx", "/old/", "/new"],
                                                volumeMounts: [
                                                    { name: "old", mountPath: "/old" },
                                                    { name: "new", mountPath: "/new" }
                                                ]
                                            }
                                        ],
                                        volumes: [
                                            { name: "old", persistentVolumeClaim: { claimName: `csc-${runID}-old-data` } },
                                            { name: "new", persistentVolumeClaim: { claimName: pvc!.metadata?.name! } },
                                        ]
                                    }
                                }
                            }
                        });

                        let succeeded = false;

                        while (true)
                        {
                            let jobStatus = await batchAPI.readNamespacedJobStatus(`csc-${runID}-copy-data`, pvc!.metadata?.namespace!);
                            
                            if ((jobStatus.body.status?.failed || 0) > 0)
                            {
                                break;
                            }
                            else if ((jobStatus.body.status?.succeeded || 0) > 0)
                            {
                                succeeded = true;
                                break;
                            }

                            process.stdout.clearLine(0);
                            process.stdout.cursorTo(0);
                            output(`    Waiting for the Job ${`csc-${runID}-copy-data`.blue} to complete.`);

                            await new Promise((resolve, reject) => setTimeout(resolve, 1000));
                        }

                        output(`\n`);

                        return succeeded;
                    } },
                    { description: `Delete the temporary PVC. Note that we do not delete the old PV - you can do this manually after you have checked that your data is intact in the new PV.`, run: async () => {
                        await coreAPI.deleteNamespacedPersistentVolumeClaim(`csc-${runID}-old-data`, pvc!.metadata?.namespace!);
                        return true;
                    } }
                ];

                output(`${`If you approve, we will carry out the following steps. You will be given the opportunity to stop the process after each step.`.underline}\n`);

                for (let i = 0; i < steps.length; i++)
                {
                    let step = steps[i];
                    output(`    ${i + 1}. ${step.description}\n`);
                }
                
                output(`\n`);

                for (let i = 0; i < steps.length; i++)
                {
                    let step = steps[i];

                    output(`Ready to run Step ${i + 1}: ${step.description}\n`);
                    
                    if (await input('    Do you want to continue? (Y/N) ') != 'Y')
                    {
                        return;
                    }

                    try
                    {
                        let successful = await step.run();

                        if (successful)
                        {
                            output(`    Step ${i + 1} was successful.\n`);
                        }
                        else
                        {
                            output(`    Step ${i + 1} failed. Quitting.\n`);
                            return false;
                        }
                    }
                    catch (e)
                    {
                        output(`    Step ${i + 1} failed${(e instanceof k8s.HttpError) ? ` (error: ${JSON.stringify(e.body)})` : ``}. Quitting.\n`);
                        return false;
                    }
                }

                output(`\n`);
                output(`Finished! Now you can recreate your Pods!\n`);
            }
        }
    });

    cmd.run(cmd.binary(app), process.argv);
}

main();
