package hu.unimiskolc.iit.distsys;



import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
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
		VirtualMachine vm0 = null;
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
			vm0 = iaas.requestVM(va, cc, r, 1)[0];
			vm0.subscribeStateChange(this);
			vmsWithPurpose.put(vm0, j);
		} catch (Exception e) {
			System.err.println("VM: ");
			System.err.println(vm0);
			System.err.println("Job: ");
			System.err.println(j);
			throw new RuntimeException(e);
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
								vmPool.remove(vm);
								vm.destroy(false);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
					});
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
		
		if (newState.equals(VirtualMachine.State.DESTROYED)) 
		{
			ComplexDCFJob job = (ComplexDCFJob) vmsWithPurpose.get(vm);
			
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
			
			if( (success + 1) / total - availability > availability + (1 - availability) * 0.5)
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
		}
	}
}
