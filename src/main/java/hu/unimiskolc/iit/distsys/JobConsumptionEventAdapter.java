package hu.unimiskolc.iit.distsys;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;

public class JobConsumptionEventAdapter extends ConsumptionEventAdapter 
{
	Job job;
	
	public JobConsumptionEventAdapter(Job job) 
	{
		super();
		this.job = job;
	}
	
	@Override
	public void conComplete() 
	{
		job.completed();
	}
}
