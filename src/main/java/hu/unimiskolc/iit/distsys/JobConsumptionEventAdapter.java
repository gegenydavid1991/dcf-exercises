package hu.unimiskolc.iit.distsys;

import java.util.Collection;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;

public class JobConsumptionEventAdapter extends ConsumptionEventAdapter 
{
	VirtualMachine vm;
	Collection<VirtualMachine> vms;
	IaaSService iaas;
	
	public JobConsumptionEventAdapter(VirtualMachine vm, Collection<VirtualMachine> vms, IaaSService iaas) 
	{
		super();
		this.vm = vm;
		this.vms = vms;
		this.iaas = iaas;
	}
	
	@Override
	public void conComplete() 
	{
		new DeferredEvent(25000) 
		{
			@Override
			protected void eventAction() 
			{
				try 
				{
					if(vm != null && vm.underProcessing.size() < 1)
					{
						vms.remove(vm);
						iaas.terminateVM(vm, true);
						System.out.println("VM destroyed.");
						System.out.println("--------------------");
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		};
	}
}
