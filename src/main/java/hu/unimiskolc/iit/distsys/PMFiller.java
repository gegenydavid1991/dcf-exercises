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
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;

public class PMFiller implements FillInAllPMs
{
	ArrayList<PhysicalMachine> orderedPMs = new ArrayList<PhysicalMachine>();

	@Override
	public void filler(IaaSService iaas, int vmCount) 
	{
		int remainingVMs = vmCount;
		
		ResourceConstraints rc;
		VirtualAppliance va = (VirtualAppliance) iaas.repositories.get(0).lookup("mainVA");
		
		Timed.simulateUntilLastEvent();
		
		for(int i = 0; i < iaas.machines.size(); i++)
		{
			orderedPMs.add(iaas.machines.get(i));
		}
		
		Collections.sort(orderedPMs, new PMComparator());
		
		for(int i = 0; i < orderedPMs.size() - 1; i++)
		{
			OccupyPM( iaas, orderedPMs.get(i) );
			remainingVMs--;
		}
				
		AlterableResourceConstraints arc = new AlterableResourceConstraints(orderedPMs.get(orderedPMs.size() - 1).freeCapacities);
		arc.multiply(1.0 / (double) remainingVMs);
		rc = new ConstantConstraints(arc);
		
		try 
		{
			iaas.requestVM((VirtualAppliance) iaas.repositories.get(0).lookup("mainVA"), rc, iaas.repositories.get(0), 90);
			remainingVMs -= 90;
		} 
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		rc = new ConstantConstraints(orderedPMs.get(orderedPMs.size() - 1).freeCapacities.getRequiredCPUs(),
				orderedPMs.get(orderedPMs.size() - 1).freeCapacities.getRequiredProcessingPower(), 1);
		remainingVMs--;
		
		OccupyPM( iaas, orderedPMs.get(orderedPMs.size() - 1) );
		
		for(VirtualMachine vm : iaas.listVMs())
		{
		}
		
		}

	private void OccupyPM(IaaSService iaas, PhysicalMachine pm)
	{
		while( pm.freeCapacities.getRequiredCPUs() >= 0.00000001)
		{
			ResourceConstraints rc = new ConstantConstraints(pm.freeCapacities.getRequiredCPUs(), pm.freeCapacities.getRequiredProcessingPower(), 1);
			
			try 
			{
				VirtualMachine vm = iaas.requestVM((VirtualAppliance) iaas.repositories.get(0).lookup("mainVA"), rc, iaas.repositories.get(0), 1)[0];
				Timed.simulateUntilLastEvent();
				
				if(pm.freeCapacities.getRequiredCPUs() >= 0.00000001)
				{ 
					iaas.terminateVM(vm, true);
					Timed.simulateUntilLastEvent();
				}
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
