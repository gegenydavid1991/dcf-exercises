package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;
import java.util.Collections;
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
		AlterableResourceConstraints arc;
		VirtualAppliance va = (VirtualAppliance) iaas.repositories.get(0).lookup("mainVA");
		
		Timed.simulateUntilLastEvent();
		
		ArrayList<PhysicalMachine> orderedPMs = new ArrayList<PhysicalMachine>();
		
		for(int i = 0; i < iaas.machines.size(); i++)
		{
			orderedPMs.add(iaas.machines.get(i));
		}
		
		Collections.sort(orderedPMs, new PMComparator());
		
		for(int i = 0; i < orderedPMs.size() - 1; i++)
		{
			arc = new AlterableResourceConstraints(orderedPMs.get(i).freeCapacities.getRequiredCPUs(), orderedPMs.get(i).freeCapacities.getRequiredProcessingPower(), 1);
			
			try 
			{
				VirtualMachine vm = iaas.requestVM((VirtualAppliance) iaas.repositories.get(0).lookup("mainVA"), arc, iaas.repositories.get(0), 1)[0];
				Timed.simulateUntilLastEvent();

				System.out.println(orderedPMs.get(i));
				System.out.println(vm.getResourceAllocation().getHost());
				System.out.println(vm);
				System.out.println("___________________");
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		
		Timed.simulateUntilLastEvent();
				
		arc = new AlterableResourceConstraints(orderedPMs.get(orderedPMs.size() - 1).freeCapacities);
		arc.multiply(1.0 / 91.0);
		
		try 
		{
			iaas.requestVM((VirtualAppliance) iaas.repositories.get(0).lookup("mainVA"), arc, iaas.repositories.get(0), 91);
		} 
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		Timed.simulateUntilLastEvent();
		
		for(int i = 0; i < iaas.machines.size(); i++)
		{
			System.out.println();
			
			System.out.println(iaas.machines.get(i).toString());
			System.out.println(iaas.machines.get(i).freeCapacities.toString());
			
			for(VirtualMachine vm : iaas.machines.get(i).listVMs())
			{
				System.out.println(vm);
			}
			
			System.out.println();
			System.out.println("--------------------");
		}
	}

}
