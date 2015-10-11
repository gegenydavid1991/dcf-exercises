package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class JobConsumptionEventAdapter extends ConsumptionEventAdapter 
{
	Job job;
	ArrayList<Job> jobs;
	BasicJobScheduler scheduler;
	
	public JobConsumptionEventAdapter(Job job, ArrayList<Job> jobs, BasicJobScheduler scheduler) 
	{
		super();
		this.job = job;
		this.jobs = jobs;
		this.scheduler = scheduler;
	}
	
	@Override
	public void conComplete() 
	{
		job.completed();
		if( !jobs.isEmpty() )
		{
			Job j = jobs.remove(0);
			scheduler.handleJobRequestArrival(j);
		}
	}
}
