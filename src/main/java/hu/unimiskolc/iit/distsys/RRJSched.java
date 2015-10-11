package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class RRJSched implements BasicJobScheduler
{
	VirtualMachine[] vms = new VirtualMachine[100];
	ArrayList<Job> jobs = new ArrayList<Job>(); 
	
	@Override
	public void setupVMset(Collection<VirtualMachine> vms) 
	{
		vms.toArray(this.vms);
	}

	@Override
	public void handleJobRequestArrival(Job j) 
	{	
		for(VirtualMachine vm : vms)
		{
			if(vm.underProcessing.size() < 1)
			{
				try 
				{
					vm.newComputeTask(j.getExectimeSecs(), j.nprocs,
							new JobConsumptionEventAdapter(j, jobs, this));
					j.started();
					return;
				} 
				catch (NetworkException e) 
				{
					e.printStackTrace();
				}
			}
		}
		
		jobs.add(j);
	}

}
