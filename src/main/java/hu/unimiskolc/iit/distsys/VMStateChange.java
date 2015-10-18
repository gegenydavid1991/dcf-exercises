package hu.unimiskolc.iit.distsys;

import java.util.Collection;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;


public class VMStateChange implements StateChange
{
	ComplexDCFJob job;
	Collection<VirtualMachine> vms;
	IaaSService iaas;
	
	public VMStateChange(Job job, Collection<VirtualMachine> vms, IaaSService iaas)
	{
		this.job = (ComplexDCFJob) job;
		this.vms = vms;
		this.iaas = iaas;
	}
	
	@Override
	public void stateChanged(VirtualMachine vm, State oldState, State newState) 
	{
		try 
		{
			if(newState == State.RUNNING)
			{
				job.startNowOnVM(vm, new JobConsumptionEventAdapter(vm, vms, iaas));
				System.out.println("Job started");
				System.out.println("VMs: " + vms.size());
				System.out.println("------------------");
			}
		} 
		catch (NetworkException e) 
		{
			e.printStackTrace();
		}
	}
}
