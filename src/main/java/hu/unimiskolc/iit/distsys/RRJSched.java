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
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.ComplexDCFJob;
import hu.unimiskolc.iit.distsys.ExercisesBase;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import at.ac.uibk.dps.cloud.simulator.test.simple.cloud.VMTest;

public class RRJSched implements BasicJobScheduler, VirtualMachine.StateChange
{

	private IaaSService iaas;
	private Repository r;
	private VirtualAppliance va;
	private HashMap<VirtualMachine, Job> vmsWithPurpose = new HashMap<VirtualMachine, Job>();
	private HashMap<VirtualMachine, DeferredEvent> vmPool = new HashMap<VirtualMachine, DeferredEvent>();
	public static int[] availabilitySuccesses = new int[TestHighAvailability.availabilityLevels.length];
	public static int[] availabilityFailures = new int[TestHighAvailability.availabilityLevels.length];
	public static int[] jobCount = new int[TestHighAvailability.availabilityLevels.length];
	
	private ArrayList<Job> jobsToRun = new ArrayList<Job>();

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
			jobCount[i] = 0;
		}
	}

	public void handleJobRequestArrival(Job j)
	{		
		boolean restarted = isRestarted(j);
		
		if(restarted || shouldRun(j))
		{
			if(!restarted)
			{
				jobsToRun.add(j);
				int index = getAvailabilityIndex(j);
				availabilitySuccesses[index]++;
				
				jobCount[getAvailabilityIndex(j)]++;
			}
			
			ConstantConstraints cc = new ConstantConstraints(j.nprocs, ExercisesBase.minProcessingCap,
					ExercisesBase.minMem / j.nprocs);
			
			Iterator<VirtualMachine> iter = vmPool.keySet().iterator();
			
			while(iter.hasNext())
			{
				VirtualMachine vm = iter.next();
				
				if(vm.getState() == State.DESTROYED)
				{
					vmPool.get(vm).cancel();
					iter.remove();
					continue;
				}
				
				if (vm.getResourceAllocation().allocated.getRequiredCPUs() >= j.nprocs)
				{
					vmPool.get(vm).cancel();
					iter.remove();
					allocateVMforJob(vm, j);
					return;
				}
			}

			VirtualMachine vm =null;
			try
			{
				vm = iaas.requestVM(va, cc, r, 1)[0];
			} 
			catch (VMManagementException e)
			{
				e.printStackTrace();
			} 
			catch (NetworkException e)
			{
				e.printStackTrace();
			}

			vm.subscribeStateChange(this);
			vmsWithPurpose.put(vm, j);
		}
		else
		{
			int index = getAvailabilityIndex(j);
			availabilityFailures[index]++;
			jobCount[getAvailabilityIndex(j)]++;
		}
	}
	
	private void allocateVMforJob(final VirtualMachine vm, Job j)
	{
		try
		{
			final ComplexDCFJob job = (ComplexDCFJob) j; 
			
			job.startNowOnVM(vm, new ConsumptionEventAdapter()
			{
				@Override
				public void conComplete()
				{
					super.conComplete();
					
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
					
					Job newJob = new ComplexDCFJob(job);
					
					handleJobRequestArrival(newJob);
				}
			});
		} 
		catch (Exception e)
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
		
		if(job == null)
		{
			return false;
		}
		
		double availability = job.getAvailabilityLevel();
		int index = getAvailabilityIndex(job);

		int success = availabilitySuccesses[index];
		int total = availabilitySuccesses[index] + availabilityFailures[index];

		double interval = 1 - availability;

		boolean result;
		
		// Check if failure would cause the availability to go too low.
		if ((double) success / (total + 1) < availability - interval / 2)
		{
			// If so, the job should run.
			result = true;
		}
		// If not, check if success would cause availability to go too high.
		else if ((double) (success + 1) / (total + 1) > availability + interval / 2)
		{
			// If so, job should not run.
			result = false;
		} 
		else
		{
			// Otherwise the job would stay in the allowed interval, so it can run.
			result = true;
		}
		
		return result;
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
	
	private boolean isRestarted(Job job)
	{
		for(Job j : jobsToRun)
		{
			if(j.getId().equals(job.getId()))
			{
				return true;
			}
		}
		
		return false;
	}
}