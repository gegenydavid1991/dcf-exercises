package hu.unimiskolc.iit.distsys;

import java.util.Comparator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;

public class PMComparator implements Comparator<PhysicalMachine> 
{
    @Override
    public int compare(PhysicalMachine pm1, PhysicalMachine pm2) 
    {
    	double cpu1 = pm1.freeCapacities.getRequiredCPUs();
    	double cpu2 = pm2.freeCapacities.getRequiredCPUs();
    	
    	double tpp1 = pm1.freeCapacities.getRequiredProcessingPower();
    	double tpp2 = pm2.freeCapacities.getRequiredProcessingPower();
    	
    	
    	if(cpu1 * tpp1 < cpu2 * tpp2)
    	{
    		return 1;
    	}
    	
    	if(cpu1 * tpp1 > cpu2 * tpp2)
    	{
    		return -1;
    	}
    	
    	return 0;
    }
}
