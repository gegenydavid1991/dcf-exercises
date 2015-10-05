package hu.unimiskolc.iit.distsys;

import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;

public class PMFiller implements FillInAllPMs
{

	@Override
	public void filler(IaaSService iaas, int vmCount) 
	{
		Timed.simulateUntilLastEvent();
		
		VirtualAppliance va = new VirtualAppliance("VAGD", 1, 0);
		AlterableResourceConstraints arc;
		
		for (int j = 0; j < 9; j++) 
		{
			int maxIndex = 0;
			for (int i = 1; i < iaas.machines.size(); i++)
			{
				if (iaas.machines.get(i).freeCapacities.getRequiredCPUs() > iaas.machines.get(maxIndex).freeCapacities
						.getRequiredCPUs()) {
					maxIndex = i;
				}
			}
			iaas.machines.get(maxIndex).localDisk.registerObject(va);
			arc = new AlterableResourceConstraints(iaas.machines.get(maxIndex).freeCapacities.getRequiredCPUs(), 1, 1);
			try 
			{
				iaas.requestVM(va, arc, iaas.machines.get(maxIndex).localDisk, 1);
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			} 
		}
		
		
		
		Timed.simulateUntilLastEvent();
				
		for(int i = 0; i < iaas.machines.size(); i++)
		{
			System.out.println(iaas.machines.get(i).toString());
			
			System.out.println("--------------------");
		}
	}

}
