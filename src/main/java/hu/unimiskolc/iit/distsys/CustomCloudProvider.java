package hu.unimiskolc.iit.distsys;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class CustomCloudProvider implements CloudProvider, VMManager.CapacityChangeEvent<PhysicalMachine>
{

	IaaSService myProvidedService;

	@Override
	public void setIaaSService(IaaSService iaas)
	{
		myProvidedService = iaas;
		myProvidedService.subscribeToCapacityChanges(this);
		((IaaSForwarder) myProvidedService).setQuoteProvider(this);
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
					myProvidedService.registerHost(ExercisesBase.getNewPhysicalMachine(RandomUtils.nextDouble(2, 5)));
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
		return rc.getTotalProcessingPower() / 20000000000l;
	}

}
