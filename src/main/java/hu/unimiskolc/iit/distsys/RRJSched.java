package hu.unimiskolc.iit.distsys;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;
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

public class RRJSched implements BasicJobScheduler, VirtualMachine.StateChange
{

	private IaaSService iaas;
	private Repository r;
	private VirtualAppliance va;
	private HashMap<VirtualMachine, Job> vmsWithPurpose = new HashMap<VirtualMachine, Job>();
	private HashMap<VirtualMachine, DeferredEvent> vmPool = new HashMap<VirtualMachine, DeferredEvent>();
	private int[] availabilitySuccesses = new int[TestHighAvailability.availabilityLevels.length];
	private int[] availabilityFailures = new int[TestHighAvailability.availabilityLevels.length];

	public void setupVMset(Collection<VirtualMachine> vms)
	{
		// ignore
	}

	public void setupIaaS(IaaSService iaas)
	{
		this.iaas = iaas;
		r = iaas.repositories.get(0);
		va = (VirtualAppliance) r.contents().iterator().next();

		for (int i = 0; i < TestHighAvailability.availabilityLevels.length; i++)
		{
			availabilityFailures[i] = 0;
			availabilitySuccesses[i] = 0;
		}
	}

	public void handleJobRequestArrival(Job j)
	{
		try
		{
			if(shouldRun(j))
			{
				ConstantConstraints cc = new ConstantConstraints(j.nprocs, ExercisesBase.minProcessingCap,
						ExercisesBase.minMem / j.nprocs);
				for (VirtualMachine vm : vmPool.keySet())
				{
					if (vm.getResourceAllocation().allocated.getRequiredCPUs() >= j.nprocs)
					{
						vmPool.remove(vm).cancel();
						allocateVMforJob(vm, j);
						return;
					}
				}
				VirtualMachine vm = iaas.requestVM(va, cc, r, 1)[0];
				vm.subscribeStateChange(this);
				vmsWithPurpose.put(vm, j);
			}
			else
			{
				if(j.getRealqueueTime() < 0)
				{
					// Start and kill job if needed.
					ConstantConstraints cc = new ConstantConstraints(j.nprocs, ExercisesBase.minProcessingCap,
							ExercisesBase.minMem / j.nprocs);
					
					VirtualMachine vm = iaas.requestVM(va, cc, r, 1)[0];
					
					final ComplexDCFJob job = (ComplexDCFJob) j;
					
					vm.subscribeStateChange(new StateChange()
					{
						@Override
						public void stateChanged(VirtualMachine vm, State oldState, State newState)
						{
							if(newState == State.RUNNING)
							{
								try
								{
									allocateVMforFun(vm, job);
									vm.destroy(true);
								}
								catch (VMManagementException e)
								{
									e.printStackTrace();
								}
							}
						}
					});
				}
				
				int index = getAvailabilityIndex(j);
				availabilityFailures[index]++;
			}
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	private void allocateVMforFun(final VirtualMachine vm, final Job j)
	{
		try
		{
			((ComplexDCFJob) j).startNowOnVM(vm, new ConsumptionEventAdapter()
			{
				@Override
				public void conComplete()
				{
					super.conComplete();
					System.err.println("Should not get here");
				}
				
				@Override
				public void conCancelled(ResourceConsumption problematic)
				{
					super.conCancelled(problematic);
					System.out.println("Job was \"not started\".");
				}
			});
			
			vm.destroy(true);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	private void allocateVMforJob(final VirtualMachine vm, final Job j)
	{
		try
		{
			((ComplexDCFJob) j).startNowOnVM(vm, new ConsumptionEventAdapter()
			{
				@Override
				public void conComplete()
				{
					super.conComplete();
					
					int index =	getAvailabilityIndex(j);
					
					availabilitySuccesses[index]++;
					
					vmPool.put(vm, new DeferredEvent(ComplexDCFJob.noJobVMMaxLife - 1000)
					{
						protected void eventAction()
						{
							try
							{
								vmPool.remove(vm);
								vm.destroy(false);
							} catch (Exception e)
							{
								throw new RuntimeException(e);
							}
						}
					});
				}
				
				@Override
				public void conCancelled(ResourceConsumption problematic)
				{
					super.conCancelled(problematic);
					handleJobRequestArrival(j);
				}
			});
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}

	}

	public void stateChanged(final VirtualMachine vm, State oldState, State newState)
	{
		if (newState.equals(VirtualMachine.State.RUNNING))
		{
			allocateVMforJob(vm, vmsWithPurpose.remove(vm));
			vm.unsubscribeStateChange(this);
		}
	}

	private boolean shouldRun(Job j)
	{
		ComplexDCFJob job = (ComplexDCFJob) j;
		
		double availability = job.getAvailabilityLevel();
		int index = getAvailabilityIndex(job);

		int success = availabilitySuccesses[index];
		int total = availabilitySuccesses[index] + availabilityFailures[index];

		double interval = 1 - availability;

		// Check if failure would cause the availability to go too low.
		if ((double) success / (total + 1) < availability - interval / 2)
		{
			// If so, the job should run.
			return true;
		}
		// If not, check if success would cause availability to go too high.
		else if ((double) (success + 1) / (total + 1) > availability + interval / 2)
		{
			// If so, job should not run.
			return false;
		} else
		{
			// Otherwise the job would stay in the allowed interval, so it can
			// run.
			return true;
		}
	}
	
	private int getAvailabilityIndex(Job j)
	{
		// Convert to ComplexDCFJob.
		ComplexDCFJob job = (ComplexDCFJob) j;

		double availability = job.getAvailabilityLevel();
		int index = 0;

		// Get the index associated with the job's availability level.
		for (int i = 0; i < TestHighAvailability.availabilityLevels.length; i++)
		{
			if (availability == TestHighAvailability.availabilityLevels[i])
			{
				index = i;
			}
		}
		
		return index;
	}
}