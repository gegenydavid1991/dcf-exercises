package hu.unimiskolc.iit.distsys;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.StateChangeListener;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder.VMListener;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class CustomCloudProvider implements CloudProvider, VMManager.CapacityChangeEvent<PhysicalMachine>, 
VMListener, StateChange
{

	IaaSService myProvidedService;
	int vmsRequested = 0;
	int vmsDestroyedByUser = 0;
	HashMap<VirtualMachine, PhysicalMachine> vmHosts = new HashMap<VirtualMachine, PhysicalMachine>();
	VMListener otherListener = null;
	
	@Override
	public void setIaaSService(IaaSService iaas)
	{
		myProvidedService = iaas;
		myProvidedService.subscribeToCapacityChanges(this);
		((IaaSForwarder) myProvidedService).setQuoteProvider(this);
		
		((IaaSForwarder) myProvidedService).setVMListener(this);
	}

	@Override
	public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity)
	{
		final boolean newRegistration = myProvidedService.isRegisteredHost(affectedCapacity.get(0));
		if (!newRegistration)
		{
			try
			{
				for (PhysicalMachine pm : affectedCapacity)
				{
					// For every lost PM we buy a new one.
					PhysicalMachine newPM = ExercisesBase.getNewPhysicalMachine(RandomUtils.nextDouble(2, 5));
					myProvidedService.registerHost(newPM);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public double getPerTickQuote(ResourceConstraints rc)
	{
		return getMySuccessRatio() * rc.getTotalProcessingPower() / 20000000000l;
	}

	@Override
	public void newVMadded(VirtualMachine[] vms)
	{		
		vmsRequested += vms.length;
		
		for(VirtualMachine vm : vms)
		{
			vm.subscribeStateChange(this);
		}
	}

	@Override
	public void stateChanged(VirtualMachine vm, VirtualMachine.State oldState,
			VirtualMachine.State newState)
	{
		if(newState == VirtualMachine.State.RUNNING)
		{
			vmHosts.put(vm, vm.getResourceAllocation().getHost());
		}
		
		if(newState == VirtualMachine.State.DESTROYED)
		{
			PhysicalMachine host = vmHosts.get(vm);
			
			// If the host is not found in the list, simply return.
			if(host == null)
			{
				return;
			}
			
			// If the host PM is still registered, the VM was surely destroyed by the user.
			if(myProvidedService.isRegisteredHost(host))
			{
				vmsDestroyedByUser++;
			}
		}
		
	}
	
	private double getMySuccessRatio()
	{
		return vmsRequested < 20 ? 1 : (double) vmsDestroyedByUser / vmsRequested; 
	}

	private VMListener getVMListener() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
	{
		IaaSForwarder service = (IaaSForwarder) myProvidedService;
		Field field = service.getClass().getDeclaredField("notifyMe");
		field.setAccessible(true);
		return (VMListener) field.get(service);
	}
}
