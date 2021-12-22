/**
 *  Represents the event messages originator, from where queued events should be retrieved.
 *  
 */
package teco.eventMessage.origin;

import teco.eventMessage.collector.EventCollectorGroup;
import teco.eventMessage.processor.target.EventMessageTarget;

/**
 * @author u190438
 *
 */
public class EventMessageOrigin {
	private long id;
	private String name;
	private int eventCollectorGroupOrder;
	private String store_command;
	private String db_driver;
	private String queue_url;
	private String user;
	private String password;
	private String consumer;
	private EventCollectorGroup group;

	private EventMessageTarget target;

	public static String db_type = "EventMessageOrigin";

	/**
	 * Create a new basic instance with passed params.
	 * The created instance was partially configured.
	 *  
	 */
	public EventMessageOrigin(long _id, String _name, int _eventCollectorGroupOrder, EventMessageTarget _target) {
		id = _id;
		name = _name;
		eventCollectorGroupOrder = _eventCollectorGroupOrder;
		target = _target;
	}

	/**
	 * Getters and setters
	 * */

	public String getStore_command() {
		return store_command;
	}

	public void setStore_command(String store_command) {
		this.store_command = store_command;
	}

	public String getDb_driver() {
		return db_driver;
	}

	public void setDb_driver(String db_driver) {
		this.db_driver = db_driver;
	}

	public String getQueue_url() {
		return queue_url;
	}

	public void setQueue_url(String queue_url) {
		this.queue_url = queue_url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getConsumer() {
		return consumer;
	}

	public void setConsumer(String consumer) {
		this.consumer = consumer;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public EventMessageTarget getTarget() {
		return target;
	}

	public int getEventCollectorGroupOrder() {
		return eventCollectorGroupOrder;
	}

	public String getName() {
		return name;
	}

	public EventCollectorGroup getGroup() {
		return group;
	}

	public void setGroup(EventCollectorGroup group) {
		this.group = group;
	}
}
