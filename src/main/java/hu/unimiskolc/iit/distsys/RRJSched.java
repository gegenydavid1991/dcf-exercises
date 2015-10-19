package hu.unimiskolc.iit.distsys;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;

import at.ac.uibk.dps.cloud.simulator.test.simple.DeferredEventTest;
import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.NoSuchVMException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class RRJSched implements BasicJobScheduler
{
	boolean scaler = true;
	
	IaaSService iaas;
	ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
	ArrayList<PhysicalMachine> orderedPMs = new ArrayList<PhysicalMachine>();
	
	int count = 0;
	
	@Override
	public void setupVMset(Collection<VirtualMachine> vms) 
	{
		vms = new ArrayList<VirtualMachine>(vms);
		scaler = false;
	}
	
	@Override
	public void setupIaaS(IaaSService iaas) 
	{
		this.iaas = iaas;
		
		orderedPMs = new ArrayList<PhysicalMachine>(iaas.machines);
		Collections.sort(orderedPMs, new PMComparator());
		
		double max = 0;
		
		for(int i = 0; i < orderedPMs.size(); i++)
		{
			double temp = orderedPMs.get(i).freeCapacities.getTotalProcessingPower();
			if(temp > max)
			{
				max = temp;
			}
		}
		
		System.out.println("Max PM: " + max);
		
		scaler = true;
	}

	@Override
	public void handleJobRequestArrival(Job j)
	{
		if(scaler)
		{
			handleScalerJobRequestArrival(j);
		}
		else
		{
			handleFillerJobRequestArrival(j);
		}
	}

	private void handleScalerJobRequestArrival(Job j) 
	{	
		VirtualMachine vm = null;
		final ComplexDCFJob job = (ComplexDCFJob) j;
		
		vm = getHandlingVM(job);
			
		if(vm == null)
		{
			try 
			{
				vm = createNewVM(job);
				vm.subscribeStateChange(new VMStateChange(job, vms, iaas));
			} 
			catch (Exception e) 
			{
				System.err.println(e.getMessage());
												
				new DeferredEvent(1000) {

					@Override
					protected void eventAction() 
					{
						handleScalerJobRequestArrival(job);
					}
					
				};
				
				return;
			}
		}
		else
		{
			System.out.println("VM found");
			System.out.println(vm);
			System.out.println("----------------");
			try 
			{
				job.startNowOnVM(vm, new JobConsumptionEventAdapter(vm, vms, iaas));
			}
			catch (NetworkException e) 
			{
				e.printStackTrace();
			}
		}
			
		System.out.println("Job started " + (++count));
		System.out.println("Execution time: " + job.getExectimeSecs());
		System.out.println("Current time: " + Timed.getFireCount() / 1000);
		System.out.println("Expected finish time: " + (Timed.getFireCount() / 1000 + job.getExectimeSecs()));
		System.out.println("Expected finish time: " + (job.getStartTimeInstance() + job.getExectimeSecs()));
		System.out.println("VMs: " + vms.size());
		System.out.println("-------------------");
	}

	private void handleFillerJobRequestArrival(final Job j)
	{
		for(VirtualMachine vm : vms)
		{
			if(vm.underProcessing.size() < 1)
			{
				try
				{
					vm.newComputeTask(j.getExectimeSecs(), j.nprocs, 
							new ConsumptionEvent()
							{
								@Override
								public void conComplete() 
								{
									j.completed();
								}

								@Override
								public void conCancelled(ResourceConsumption problematic) {  }
							});
					j.started();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	private VirtualMachine createNewVM(Job job) throws VMManagementException, NetworkException
	{
		double required = job.nprocs * ExercisesBase.maxProcessingCap;
		Collections.sort(orderedPMs, new PMComparator());
		
		int availableIndex = 0;
		
		for(int i = 0; i < orderedPMs.size(); i++)
		{
			double temp = orderedPMs.get(i).freeCapacities.getTotalProcessingPower();
			if(temp > orderedPMs.get(availableIndex).freeCapacities.getTotalProcessingPower())
			{
				availableIndex = i;
			}
		}
		
		System.out.println("Job requires: " + required);
		System.out.println("Available: " + orderedPMs.get(availableIndex).freeCapacities.getTotalProcessingPower());
		System.out.println(orderedPMs.get(availableIndex).freeCapacities);
		
		
		double cpu = required / orderedPMs.get(availableIndex).freeCapacities.getTotalProcessingPower();
		
		System.out.println("CPU: " + cpu);
		System.out.println("--------------");
		
		AlterableResourceConstraints rc = new AlterableResourceConstraints(
				cpu, orderedPMs.get(availableIndex).freeCapacities.getTotalProcessingPower(), job.usedMemory
				);
		//rc.multiply(2);
		VirtualMachine vm;

		vm = iaas.requestVM((VirtualAppliance) iaas.repositories.get(0).lookup("mainVA"), rc,
				iaas.repositories.get(0), 1)[0];
			
		vms.add(vm);
		return vm;
	}
	
	private VirtualMachine getHandlingVM(Job job)
	{
		VirtualMachine handler = null;
		AlterableResourceConstraints rc = new AlterableResourceConstraints(job.nprocs, job.perProcCPUTime, job.usedMemory);
		//rc.multiply(2);
		
		for(VirtualMachine vm : vms)
		{
			if(vm.getResourceAllocation().allocated.getRequiredCPUs() * vm.getResourceAllocation().allocated.getRequiredProcessingPower()
					> rc.getRequiredCPUs() * rc.getRequiredProcessingPower()
					&& vm.underProcessing.size() == 0
					&& vm.getState() == State.RUNNING)
			{
				handler = vm;
				break;
			}
		}
		
		return handler;
	}
}
