package org.fhcrc.centernet.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.webconsole.plugins.event.internal.OsgiUtil;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import com.day.cq.replication.ReplicationActionType;
import com.day.cq.replication.ReplicationException;
import com.day.cq.replication.Replicator;
import com.day.cq.search.PredicateGroup;
import com.day.cq.search.QueryBuilder;
import com.day.cq.search.result.Hit;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.day.cq.wcm.api.Page;
import com.day.cq.wcm.api.PageManager;
import com.day.cq.wcm.api.WCMException;

import org.fhcrc.centernet.Constants;

@Service(value = java.lang.Runnable.class)
@Component(name = "org.fhcrc.centernet.service.LMSEventImporter", 
				label = "Fred Hutch - Cornerstone Event Importer", 
				description = "Service to import the training events from Fred Hutch's Learning Management System into CenterNet", 
				metatype = true)
@Properties({
	@Property(name = "service.vendor", value = "Fred Hutch", propertyPrivate = true),		
	@Property(name = "scheduler.concurrent", label = "Allow concurrent executions", description = "Allow concurrent executions of this scheduled service", 
		boolValue = false, propertyPrivate = true),
	@Property(name = "scheduler.enabled", label = "Job Enabled?", description = "Turn on or off the schedule job", 
		boolValue = true, propertyPrivate = true),	
	@Property(name = "scheduler.expression", label = "LMS Importer Cron Expression", 
		description = "Cron expression to determine when the LMS Importer will be initiated.", 
		value = "0 0/1 * * * ? *"),
	@Property(name = "service.target", value= "/content/centernet/en/e/training/lms-import", 
		description = "Location in JCR where new events should be placed", label = "Importer target location"),
	@Property(name = "service.dataSource", value="https://is.fhcrc.org/sites/centernet/lms/full.txt",
		description = "URL at which to find the tab-separated data file for this service to ingest",
		label = "Data source URL")
})
public class LMSEventImporter implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(LMSEventImporter.class);
	private static final String LOGGING_PREFIX = "LMS IMPORTER: ";
	private static final String TARGET_PATH_DEFAULT = "/content/centernet/en/e/training/lms-import";
	private static final String DEFAULT_PAGE_NAME = "imported-training-event";
	private static final String PROP_SCAFFOLDING = "cq:scaffolding";
	private static final String PROP_RESOURCE_TYPE = "sling:resourceType";
	private static final String EVENT_SCAFFOLDING_TEMPLATE = "/etc/scaffolding/centernet/event-scaffoling";
	private static final String EVENT_COMPONENT_RES = "centernet/components/page/event";
	private static final String EVENT_DETAILS_NODE = "eventdetails";
	private static final String EVENT_BUTTON_NODE = "button";
	private static final String PN_EVENT_DESCRIPTION = "text";
	private static final String PN_EVENT_LOCATION = "location";
	private static final String PN_EVENT_HOST = "host";
	private static final String PN_EVENT_CONTACT_NAME = "contactName";
	private static final String PN_EVENT_CONTACT_EMAIL = "contactEmail";
	private static final String PN_EVENT_CONTACT_PHONE = "contactPhone";
	private static final String PN_EVENT_SUMMARY = "jcr:description";
	private static final String PN_EVENT_START = "start";
	private static final String PN_EVENT_END = "end";
	private static final String PN_EVENT_BUTTON_TEXT = "text";
	private static final String EVENT_BUTTON_TEXT_VALUE = "Register";
	private static final String PN_EVENT_BUTTON_URL = "linkUrl";
	private static final String PN_EVENT_UID = "eventId";
	
	/* The fields we expect to get from the data file and their order */
	private final Integer TITLE = 0;
	private final Integer LOCATOR_NUMBER = 1;
	private final Integer DESCRIPTION = 2;
	private final Integer LOCATION = 3;
	private final Integer HOST = 4;
	private final Integer CONTACT_NAME = 5;
	private final Integer CONTACT_EMAIL = 6;
	private final Integer CONTACT_PHONE = 7;
	private final Integer START_DATE = 8;
	private final Integer START_TIME = 9;
	private final Integer END_DATE = 10;
	private final Integer END_TIME = 11;
	private final Integer IS_ACTIVE = 12;
	private final Integer UID = 13;
	private final Integer SUMMARY = 14;
	private final Integer OPT_IN = 15;
	private final Integer DEEP_LINK = 16;
	
	/* Number of fields we expect so we can check each line before we commit to importing it */
	private final Integer EXPECTED_NUMBER_OF_FIELDS = 17;
	
	private String targetPath;
	private String dataSource;
	private Session adminSession;
	private PageManager pageManager;
	private ResourceResolver resResolver;
	private Map<String, String> uidMap;
	
	@Reference
	private ResourceResolverFactory factory;
	
	@Reference
	private Replicator replicator;
	
	@Activate
	private void activate(ComponentContext context) {	
		
        log.info("Activating Fred Hutch - LMS Importer Service. " + this.getClass().getName());
        
        Dictionary<?, ?> properties = context.getProperties();
        targetPath = OsgiUtil.toString(properties, "service.target", new String());
        dataSource = OsgiUtil.toString(properties, "service.dataSource", new String());
        
    }

	@Override
	public void run() {
		
		String inputLine;
		log.info(LOGGING_PREFIX + "Starting import");
		log.info(LOGGING_PREFIX + "targetPath = " + targetPath);
		log.info(LOGGING_PREFIX + "dataSource = " + dataSource);
		
		uidMap = createUIDMap();
		
		try {
			
			URL dataurl = new URL(dataSource);
			URLConnection dataConnection = dataurl.openConnection();
			
			BufferedReader dataReader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
			
			/* Take each line as input and deal with it, but throw away the 
			 * first line as it contains only headers, not data */
			inputLine = dataReader.readLine();
			while ((inputLine = dataReader.readLine()) != null) {
				
				log.debug(LOGGING_PREFIX + "inputLine = " + inputLine);
				String[] data = inputLine.split("\t");
				if (data.length != EXPECTED_NUMBER_OF_FIELDS) {
					log.error(LOGGING_PREFIX + "Unexpected number of fields from LMS output. Aborting line starting with " + data[0]);
					continue;
				} else {
					
					if (uidMap != null && uidMap.containsKey(data[UID])) {
						deleteEventNode(data);
					}
					createEventNode(data);
				}
				
			}
			
		} catch (Exception e) {
			log.error(LOGGING_PREFIX, e);
		}
		
	}
	
	/**
	 * 
	 * @param dateString String representing the date. Should be in the form 'MM/DD/YYYY'
	 * @param timeString String representing the time. Should be in the form 'HH:mm:ss'
	 * @return Calendar representing the date provided by the dateString and timeString or
	 * null if the parameters do not match the expected formatting
	 */
	private Calendar createDate(String dateString, String timeString) {
		
		final String DATE_FORMAT = "MM/dd/yyyy HH:mm:ss";
		SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
		Date date;
		StringBuffer sb = new StringBuffer();
		
		sb.append(dateString);
		sb.append(" ");
		sb.append(timeString);

		try {
			date = df.parse(sb.toString());
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			return cal;
		} catch (ParseException e) {
			log.error(LOGGING_PREFIX + "Incorrectly formatted date string: " + dateString + " " + timeString, e);
			return null;
		}
		
	}
	
	private String sanitizeTitle(String string) {
		String result = string.toLowerCase();
		result = result.replaceAll(" ", "-");
		result = result.replaceAll("[^a-zA-Z0-9_\\s]", "-");
		return result;
	}
	
	@SuppressWarnings("deprecation")
	private void deleteEventNode(String[] data) {
		
		String path = uidMap.get(data[UID]);
		
		if (path != null) {
			
			log.debug(LOGGING_PREFIX + "Deleting node at path " + path);
			
			try {
				
				resResolver = factory.getAdministrativeResourceResolver(null);
				adminSession = resResolver.adaptTo(Session.class);
				
				Node deleteNode = adminSession.getNode(path);
				
				if (deleteNode != null) {
					deleteNode.remove();
					adminSession.save();
				}
				
				adminSession.logout();
				
			} catch (LoginException e) {
				log.error(LOGGING_PREFIX + "Problem logging in admin session for UID Map", e);
			} catch (PathNotFoundException e) {
				log.error(LOGGING_PREFIX + "No Node found at path " + path, e);
			} catch (RepositoryException e) {
				log.error(LOGGING_PREFIX + "Problem getting Node for deletion", e);
			} finally {

				/* Double-check that we are logged out */
				if (adminSession != null) {
					adminSession.logout();
				}
				
			}
			
		}
		
	}
	
	@SuppressWarnings("deprecation")
	private void createEventNode(String[] data) {
		
		if (targetPath == null || targetPath.trim().isEmpty()) {
			targetPath = TARGET_PATH_DEFAULT;
		}
		
		Calendar startCal = Calendar.getInstance(),
				endCal = Calendar.getInstance();
		SimpleDateFormat justTheMonth = new SimpleDateFormat("MMM"),
				justTheDay = new SimpleDateFormat("dd");
		
		if (!data[START_DATE].trim().isEmpty() && !data[START_TIME].trim().isEmpty()) {
			
			startCal = createDate(data[START_DATE], data[START_TIME]);
			
			if (!data[END_DATE].trim().isEmpty() && !data[END_TIME].trim().isEmpty()) {
				endCal = createDate(data[END_DATE], data[END_TIME]);
			} else {
				/* If there was no end date, just set it to be the same as the start date */
				endCal = startCal;
			}
			
		} else {
			//TODO No start date means no event. Report this error to HR Training.
			StringBuffer sb = new StringBuffer();
			for (String s : data) {
				sb.append(s);
				sb.append("||");
			}
			
			log.error(LOGGING_PREFIX + "Event contained no start date. Data array dump: " + sb.toString());
			return;
		}
		
		try {
			
			resResolver = factory.getAdministrativeResourceResolver(null);
			adminSession = resResolver.adaptTo(Session.class);
			pageManager = resResolver.adaptTo(PageManager.class);
			
			Page eventPage = null;
			String pageName = null;
			
			String[] dateParts = data[START_DATE].split("/");
			/* Make sure we actually have a date here */
			if (dateParts.length > 1) {
				
				/* Date is in the form MM/DD/YYYY */
				String datePath = "/" + dateParts[2] + "/" + dateParts[0];
				
				Node importPathBaseNode = JcrUtils.getOrCreateByPath(targetPath, "sling:OrderedFolder", adminSession);
				Node importPathWithDate = JcrUtils.getOrCreateByPath(importPathBaseNode.getPath() + datePath, "sling:OrderedFolder", adminSession);
				
				/* This will need to be changed to a UID later */
				if (data[TITLE] != null && !data[TITLE].isEmpty()) {
					// 57 is 64 - (length of month string + length of day string + 2 hyphens)
					pageName = data[TITLE].substring(0, Math.min(data[TITLE].length(), 57));
					pageName = sanitizeTitle(pageName);
				} else {
					pageName = DEFAULT_PAGE_NAME;
				}
				
				pageName = pageName + "-" + justTheMonth.format(startCal.getTime());
				pageName = pageName + "-" + justTheDay.format(startCal.getTime()); 
				
				if (importPathWithDate.hasNode(pageName)) {
					//TODO Update node instead of create
					eventPage = null;
				} else {
					eventPage = pageManager.create(importPathWithDate.getPath(), pageName, Constants.EVENT_TEMPLATE, data[TITLE]);
					Node eventPageContentNode = eventPage.getContentResource().adaptTo(Node.class);
					/* Set the basic scaffolding and page type of the created page */
		            eventPageContentNode.setProperty(PROP_SCAFFOLDING, EVENT_SCAFFOLDING_TEMPLATE);
		            eventPageContentNode.setProperty(PROP_RESOURCE_TYPE, EVENT_COMPONENT_RES);
		            /* Create the eventdetails node */
		            Node eventDetailsNode = JcrUtils.getOrCreateByPath(eventPageContentNode.getPath() + "/" + EVENT_DETAILS_NODE, "nt:unstructured", adminSession);

		            if (startCal == null) {
		            	//TODO More drastic action should be taken here, send an email alert?
		            } else {
		            	eventDetailsNode.setProperty(PN_EVENT_START, startCal);
		            }

		            if (endCal == null) {
		            	//TODO More drastic action should be taken here, send an email alert?
		            } else {
		            	eventDetailsNode.setProperty(PN_EVENT_END, endCal);
		            }
		            
		            
		            /* Start setting properties if they exist */
		            if (!data[DESCRIPTION].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_DESCRIPTION, data[DESCRIPTION]);
		            }
		            if (!data[LOCATION].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_LOCATION, data[LOCATION]);
		            }
		            if (!data[HOST].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_HOST, data[HOST]);
		            }
		            if (!data[CONTACT_NAME].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_CONTACT_NAME, data[CONTACT_NAME]);
		            }
		            if (!data[CONTACT_EMAIL].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_CONTACT_EMAIL, data[CONTACT_EMAIL]);
		            }
		            if (!data[CONTACT_PHONE].trim().isEmpty()) {
		            	eventDetailsNode.setProperty(PN_EVENT_CONTACT_PHONE, data[CONTACT_PHONE]);
		            }
		            
		            /* The Deep Link should be rendered as a Register button */
		            if (!data[DEEP_LINK].trim().isEmpty()) {
		            	Node eventButtonNode = JcrUtils.getOrCreateByPath(eventDetailsNode.getPath() + "/" + EVENT_BUTTON_NODE, "nt:unstructured", adminSession);
		            	eventButtonNode.setProperty(PN_EVENT_BUTTON_TEXT, EVENT_BUTTON_TEXT_VALUE);
		            	eventButtonNode.setProperty(PN_EVENT_BUTTON_URL, data[DEEP_LINK]);
		            }
		            
		            
		            /* The summary is set as the jcr:description which goes on the jcr:content node */
		            if (!data[SUMMARY].trim().isEmpty()) {
		            	eventPageContentNode.setProperty(PN_EVENT_SUMMARY, data[SUMMARY]);
		            }
		            /* Store the UID on the jcr:content node as well */
		            if (!data[UID].trim().isEmpty()) {
		            	eventPageContentNode.setProperty(PN_EVENT_UID, data[UID]);
		            }
		            
				}
				
				/* Save everything */
				adminSession.save();
				
				/* Replicate the page in question */
				if (eventPage != null) {
					
					/* Replicate the page unless someone has deactivated it */
					if (!replicator.getReplicationStatus(adminSession, eventPage.getPath()).isDeactivated()) {
						
						try {
							replicator.replicate(adminSession, ReplicationActionType.ACTIVATE, eventPage.getPath());
						} catch (ReplicationException e) {
							log.error(LOGGING_PREFIX + "Problem replicating page " + eventPage.getPath(), e);
						}
						
					}
					
				}
				
				adminSession.logout();
				
			}
			
		} catch (LoginException e) {
			log.error(LOGGING_PREFIX + "Problem logging in to create imported event node", e);
		} catch (RepositoryException e) {
			log.error(LOGGING_PREFIX + "Repo Exception attempting to create imported event node", e);
		} catch (WCMException e) {
			log.error(LOGGING_PREFIX + "WCM Exception attempting to replicate imported event node", e);
		} finally {

			/* Double-check that we are logged out */
			if (adminSession != null) {
				adminSession.logout();
			}
			
		}
		
	}
	
	@SuppressWarnings("deprecation")
	private Map<String, String> createUIDMap() {
		
		Map<String, String> uidMap = new HashMap<String, String>(),
				queryMap = new HashMap<String, String>();
		
		try {
			
			resResolver = factory.getAdministrativeResourceResolver(null);
			adminSession = resResolver.adaptTo(Session.class);
			QueryBuilder qb = resResolver.adaptTo(QueryBuilder.class);
			
			/* Find all events under the targetPath */
			queryMap.put("type","cq:Page");
			queryMap.put("path", targetPath);
			queryMap.put("property","jcr:content/cq:template");
		    queryMap.put("property.value", Constants.EVENT_TEMPLATE);
		    queryMap.put("p.limit", "-1");
		    queryMap.put("p.guessTotal", "true");
		    
		    Iterator<Hit> hitList = qb.createQuery(PredicateGroup.create(queryMap), adminSession).getResult().getHits().iterator();
		    
		    while (hitList.hasNext()) {

		    	Page p = hitList.next().getResource().adaptTo(Page.class);
		    	String uid = p.getContentResource().getValueMap().get(PN_EVENT_UID, "");

		    	if (uid != null && !uid.isEmpty()) {
		    		uidMap.put(uid, p.getPath());
		    		log.debug(LOGGING_PREFIX + "Adding UID pair to map: " + uid + ", " + p.getPath());
		    	}


		    }
		    
		    adminSession.logout();

		} catch (RepositoryException e) {
			log.error(LOGGING_PREFIX + "Problem adapting hit to Page.", e);
	    } catch (LoginException e) {
	    	log.error(LOGGING_PREFIX + "Problem logging in admin session for UID Map", e);
	    } finally {
	    	
	    	/* Double-check that we are logged out */
			if (adminSession != null) {
				adminSession.logout();
			}
			
	    }
		
		return uidMap;
		
	}

}
