package hu.unimiskolc.iit.distsys;



import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.ComplexDCFJob;
import hu.unimiskolc.iit.distsys.ExercisesBase;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

import java.util.Collection;
import java.util.HashMap;

public class RRJSched implements BasicJobScheduler,
		VirtualMachine.StateChange {

	private IaaSService iaas;
	private Repository r;
	private VirtualAppliance va;
	private HashMap<VirtualMachine, Job> vmsWithPurpose = new HashMap<VirtualMachine, Job>();
	private HashMap<VirtualMachine, DeferredEvent> vmPool = new HashMap<VirtualMachine, DeferredEvent>();
	private int[] availabilitySuccesses = new int[3];
	private int[] availabilityFailures = new int[3];

	public void setupVMset(Collection<VirtualMachine> vms) {
		// ignore
	}

	public void setupIaaS(IaaSService iaas) {
		this.iaas = iaas;
		r = iaas.repositories.get(0);
		va = (VirtualAppliance) r.contents().iterator().next();
	}

	public void handleJobRequestArrival(Job j) {
		try {
			ConstantConstraints cc = new ConstantConstraints(j.nprocs,
					ExercisesBase.minProcessingCap, ExercisesBase.minMem
							/ j.nprocs);
			for (VirtualMachine vm : vmPool.keySet()) {
				if (vm.getResourceAllocation().allocated.getRequiredCPUs() >= j.nprocs) {
					vmPool.remove(vm).cancel();
					allocateVMforJob(vm, j);
					return;
				}
			}
			VirtualMachine vm = iaas.requestVM(va, cc, r, 1)[0];
			vm.subscribeStateChange(this);
			vmsWithPurpose.put(vm, j);
		} catch (Exception e) 
		{
			System.err.println("NullPointerException happened at " + Timed.getFireCount());
			System.err.println("while handling " + j);
				//handleJobRequestArrival(j);
		}
	}

	private void allocateVMforJob(final VirtualMachine vm, Job j) {
		try {
			final ComplexDCFJob job = (ComplexDCFJob) j; 
			
			job.startNowOnVM(vm, new ConsumptionEventAdapter() {
				@Override
				public void conComplete() 
				{
					super.conComplete();
					
					double availability = job.getAvailabilityLevel();
					
					for(int i = 0; i < TestHighAvailability.availabilityLevels.length; i++)
					{
						if(availability == TestHighAvailability.availabilityLevels[i])
						{
							availabilitySuccesses[i]++;
						}
					}
					
					vmPool.put(vm, new DeferredEvent(
							ComplexDCFJob.noJobVMMaxLife - 1000) 
					{
						protected void eventAction() 
						{
							try 
							{
								//vmPool.remove(vm);
								//vm.destroy(false);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
					});
				}
				
				@Override
				public void conCancelled(ResourceConsumption problematic)
				{
					super.conCancelled(problematic);
												
					double availability = job.getAvailabilityLevel();
							
					int index = 0;
							
					for(int i = 0; i < TestHighAvailability.availabilityLevels.length; i++)
					{
						if(availability == TestHighAvailability.availabilityLevels[i])
						{
							index = i;
							break;
						}
					}
							
					int total = availabilitySuccesses[index] + availabilityFailures[index];
					int success = availabilitySuccesses[index];
							
					vmsWithPurpose.remove(vm);
					vmPool.remove(vm);
							
					System.out.println((success + 1) / (total + 1) + " > " + (availability + (1 - availability) * 0.5) + "?");
					System.out.println(success / (total + 1) + " < " + (availability - (1 - availability) * 0.5) + "?");
						
					if( (success + 1) / (total + 1) > availability + (1 - availability) * 0.5
							&& !(success / (total + 1) < availability - (1 - availability) * 0.5))
					{
						System.err.println("Oh noo! Job will not complete.");
						availabilityFailures[index]++;
						return;
					}
					else
					{
						System.err.println("Oh noo! Rescheduling job.");
						handleJobRequestArrival(job);
					}
							
					for(int i = 0; i < availabilitySuccesses.length; i++)
					{
						System.out.println("Successful jobs with level " + TestHighAvailability.availabilityLevels[i] + ": " + availabilitySuccesses[i]);
						System.out.println("Failed jobs with level " + TestHighAvailability.availabilityLevels[i] + ": " + availabilityFailures[i]);
						System.out.println("------------------------------");
					}
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public void stateChanged(final VirtualMachine vm, State oldState,
			State newState) {		
		if (newState.equals(VirtualMachine.State.RUNNING)) 
		{
			allocateVMforJob(vm, vmsWithPurpose.remove(vm));
			vm.unsubscribeStateChange(this);
		}
	}
}
