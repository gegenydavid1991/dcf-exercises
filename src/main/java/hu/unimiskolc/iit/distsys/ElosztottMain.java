package hu.unimiskolc.iit.distsys;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.ResourceAllocation;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.AlwaysOnMachines;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.FirstFitScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.*;

public class ElosztottMain {

	public static void main(String[] args) throws Exception
	{
		 IaaSService iaas = ExercisesBase.getNewIaaSService();
		 
		 PhysicalMachine[] pms = new PhysicalMachine[10];
		 VirtualMachine[] vms = new VirtualMachine[100];
		 
		 for(int i = 0; i < 10; i++)
		 {
			 pms[i] = ExercisesBase.getNewPhysicalMachine();
			 pms[i].turnon();
			 iaas.registerHost(pms[i]);
			 iaas.registerRepository(pms[i].localDisk);
		 }
		 
		 VirtualAppliance va = new VirtualAppliance("VAGD", 1, 0);
		 AlterableResourceConstraints arc;
		 
		 for(int i = 0; i < 10; i++)
		 {
			 pms[i].localDisk.registerObject(va);
			 
			 double cpus = pms[i].freeCapacities.getRequiredCPUs();
			 
			 System.out.println(cpus);
			 
			 arc = new AlterableResourceConstraints(cpus / 10.0, 1, 1);
			 
			 for(int j = 0; j < 10; j++)
			 {
				 vms[i] = iaas.requestVM(va, arc, pms[i].localDisk, 1)[0]; 
			 }
		 }
		 
		 Timed.simulateUntilLastEvent();
		 
		 System.out.println("-----------------------");
		 
		 for(int i = 0; i < 10; i++)
		 {
			 System.out.println(pms[i].freeCapacities.getRequiredCPUs());
		 }
		 
		 System.out.println("ÁÁÁÁÁÁ");
	}

}
