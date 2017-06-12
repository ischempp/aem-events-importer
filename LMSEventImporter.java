package org.fhcrc.centernet.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Deactivate;
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
import com.day.cq.tagging.TagManager;
import com.day.cq.tagging.Tag;

import org.fhcrc.centernet.Constants;
import org.fhcrc.common.util.PageUtilities;
import org.fhcrc.common.services.EmailService;
import org.fhcrc.common.services.ErrorEmailService;

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
		value = "0 0 4 1/1 * ? *"),
	@Property(name = "service.target", value= "/content/centernet/en/e/lms-import", 
		description = "Location in JCR where new events should be placed", label = "Importer target location"),
	@Property(name = "service.dataSource", value="https://is.fhcrc.org/sites/centernet/lms/lms-import-data.txt",
		description = "URL at which to find the tab-separated data file for this service to ingest",
		label = "Data source URL")
})
public class LMSEventImporter implements Runnable {
	
	/* Logging and defaults */
	private static final Logger log = LoggerFactory.getLogger(LMSEventImporter.class);
	private static final String LOGGING_PREFIX = "LMS IMPORTER: ";
	private static final String TARGET_PATH_DEFAULT = "/content/centernet/en/e/lms-import";
	private static final String DEFAULT_PAGE_NAME = "imported-training-event";
	/* Scaffolding information */
	private static final String PROP_SCAFFOLDING = "cq:scaffolding";
	private static final String PROP_RESOURCE_TYPE = "sling:resourceType";
	private static final String EVENT_SCAFFOLDING_TEMPLATE = "/etc/scaffolding/centernet/event-scaffoling";
	/* Event Page and Event Details component information */
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
	private static final String TRAINING_EVENT_CATEGORY_TAG_ID = "web-event-categories:training";
	/* Error alerting contacts */
	private static final String HR_TRAINING_EMAIL_CONTACT = "hutchlearning@fredhutch.org";
	private static final String COMMUNICATIONS_EMAIL_CONTACT = "websys@fredhutch.org";
	
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
	
	/* Vendor values from Cornerstone. If new Vendors are added, they must be added here as well */
	final String PN_VENDOR_CIT = "Center IT";
	final String PN_VENDOR_CRS = "Clinical Research Support (CRS)";
	final String PN_VENDOR_COMMUNICATIONS = "Communications & Marketing";
	final String PN_VENDOR_EHS = "Environmental Health & Safety (EH&S)";
	final String PN_VENDOR_FINANCIAL_PLANNING = "Financial Planning and Analysis";
	final String PN_VENDOR_FMIS = "FMIS";
	final String PN_VENDOR_FRED_HUTCH = "Fred Hutch";
	final String PN_VENDOR_HR_TRAINING = "HR Training";
	final String PN_VENDOR_RESEARCH_ETHICS = "Hutch Research Ethics Education Program";
	final String PN_VENDOR_IRO = "Institutional Review Office (IRO)";
	final String PN_VENDOR_OSR = "Office of Sponsored Research";
	
	/* Department tag IDs for the vendors listed above */
	final String PN_TAG_CIT = "web-depts:AD/AD07";
	final String PN_TAG_CRS = "web-depts:AD/AD0103";
	final String PN_TAG_COMMUNICATIONS = "web-depts:AD/AD09";
	final String PN_TAG_EHS = "web-depts:AD/AD0303";
	final String PN_TAG_FINANCIAL_PLANNING = "web-depts:AD/AD0405";
	final String PN_TAG_FMIS = "web-depts:AD/AD04013";
	final String PN_TAG_FRED_HUTCH = "";
	final String PN_TAG_HR_TRAINING = "web-depts:AD/AD0603";
	final String PN_TAG_RESEARCH_ETHICS = "web-depts:HX/HX011";
	final String PN_TAG_IRO = "web-depts:AD/AD0101";
	final String PN_TAG_OSR = "web-depts:AD/AD0402";
	
	/* Class fields */
	private String targetPath;
	private String dataSource;
	private Session adminSession;
	private PageManager pageManager;
	private TagManager tagManager;
	private ResourceResolver resResolver;
	private Map<String, String> uidMap;
	private Map<String, String> tagMap;
	
	@Reference
	private ResourceResolverFactory factory;
	
	@Reference
	private Replicator replicator;
	
	@Reference
	private EmailService errorEmailService;
	
	@Activate
	private void activate(ComponentContext context) {	
		
        log.info("Activating Fred Hutch - LMS Importer Service. " + this.getClass().getName());
        
        Dictionary<?, ?> properties = context.getProperties();
        targetPath = OsgiUtil.toString(properties, "service.target", new String());
        dataSource = OsgiUtil.toString(properties, "service.dataSource", new String());
        
    }
	
	@Deactivate
	private void deactivate() {
		log.info(LOGGING_PREFIX + "Service deactivated");
	}

	@Override
	public void run() {
		
		String inputLine;
		log.info(LOGGING_PREFIX + "Starting import");
		log.info(LOGGING_PREFIX + "targetPath = " + targetPath);
		log.info(LOGGING_PREFIX + "dataSource = " + dataSource);
		
		uidMap = createUIDMap();
		tagMap = createTagMap();
		
		try {
			
			/* Open connection to the data source as set in the OSGi configs */
			URL dataurl = new URL(dataSource);
			URLConnection dataConnection = dataurl.openConnection();
			
			BufferedReader dataReader = new BufferedReader(new InputStreamReader(dataConnection.getInputStream()));
			
			/* Take each line as input and deal with it, but throw away the 
			 * first line as it contains only headers, not data */
			inputLine = dataReader.readLine();
			while ((inputLine = dataReader.readLine()) != null) {
				
				log.debug(LOGGING_PREFIX + "inputLine = " + inputLine);
				/* The data is a tab-delimited string */
				String[] data = inputLine.split("\t");
				/* If we have the wrong number of fields, notify HR Training and move on to the next line */
				if (data.length != EXPECTED_NUMBER_OF_FIELDS) {
					
					String errorString = createDataDump(data);
					errorEmailService.sendEmail(HR_TRAINING_EMAIL_CONTACT, 
							"AEM LMS importer error: Unexpected number of fields", 
							errorString);
					log.error(LOGGING_PREFIX + "Unexpected number of fields from LMS output. Aborting line " + errorString);
					continue;
					
				} else {
					
					createEventNode(data);
					
				}
				
			}
			
		} catch (MalformedURLException e) {
			log.error(LOGGING_PREFIX + "Incorrect URL for data file: " + dataSource);
			errorEmailService.sendEmail(COMMUNICATIONS_EMAIL_CONTACT, "LMS importer error: malformed URL for data file", e.getMessage());
		} catch (IOException e) {
			log.error(LOGGING_PREFIX + "Problem reading data file");
			errorEmailService.sendEmail(COMMUNICATIONS_EMAIL_CONTACT, "LMS importer error: cannot read data file", e.getMessage());
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
			StringBuffer errorSb = new StringBuffer();
			errorSb.append("<p style=\"line-height: 20px; font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif; font-size: 16px;\">");
			errorSb.append("I found the following date and time strings in the data file that could not be parsed by the importer:");
			errorSb.append("<br>");
			errorSb.append("Date string (must be formatted MM/dd/yyyy): ");
			errorSb.append(dateString);
			errorSb.append("<br>");
			errorSb.append("Time string (must be formatted HH:mm:ss): ");
			errorSb.append(timeString);
			errorSb.append("</p>");
			errorEmailService.sendEmail(HR_TRAINING_EMAIL_CONTACT, 
					"AEM LMS importer error: incorrectly formatted date in data file", 
					errorSb.toString());
			return null;
		}
		
	}
	
	/**
	 * Workhorse function that takes in the line of data from the file and 
	 * either creates a new Event Page or updates the Event Page whose UID 
	 * matches the one in the UID field.
	 * @param data - String[] containing EXPECTED_NUMBER_OF_FIELDS entries
	 */
	@SuppressWarnings("deprecation")
	private void createEventNode(String[] data) {
		
		if (targetPath == null || targetPath.trim().isEmpty()) {
			targetPath = TARGET_PATH_DEFAULT;
		}
		
		Calendar startCal = Calendar.getInstance(),
				endCal = Calendar.getInstance();
		SimpleDateFormat justTheMonth = new SimpleDateFormat("MMM"),
				justTheDay = new SimpleDateFormat("dd");
		
		/* If there is a Start Date and Start Time, then set the startCal Calendar */
		if (!data[START_DATE].trim().isEmpty() && !data[START_TIME].trim().isEmpty()) {
			
			startCal = createDate(data[START_DATE], data[START_TIME]);
			
			if (!data[END_DATE].trim().isEmpty() && !data[END_TIME].trim().isEmpty()) {
				endCal = createDate(data[END_DATE], data[END_TIME]);
			} else {
				/* If there was no end date, just set it to be the same as the start date */
				endCal = startCal;
			}
			
		} else {
			
			/* If there was no Start Time, we can't make a meaningful Event, so
			 * notify HR Training and move on to the next line of data. */
			String dataDump = createDataDump(data);
			
			errorEmailService.sendEmail(HR_TRAINING_EMAIL_CONTACT, "AEM LMS importer error: Event contains no Start Date", dataDump);
			log.error(LOGGING_PREFIX + "Event contained no start date. Data array dump: " + dataDump);
			return;
			
		}
		
		try {
			
			resResolver = factory.getAdministrativeResourceResolver(null);
			adminSession = resResolver.adaptTo(Session.class);
			pageManager = resResolver.adaptTo(PageManager.class);
			tagManager = resResolver.adaptTo(TagManager.class);
			
			Page eventPage = null;
			String pageName = null;
			Tag categoryTag = tagManager.resolve(TRAINING_EVENT_CATEGORY_TAG_ID),
					departmentTag = getVendorTag(data[HOST], tagManager);
			List<Tag> tagList = new ArrayList<Tag>();
			
			/* Populate the list of tags we will later add to the page */
			if (categoryTag != null) {
				tagList.add(categoryTag);
			} else {
				log.warn(LOGGING_PREFIX + "Problem resolving category tag " + TRAINING_EVENT_CATEGORY_TAG_ID);
			}
			
			if (departmentTag != null) {
				tagList.add(departmentTag);
			} else {
				log.warn(LOGGING_PREFIX + "Problem resolving vendor to department tag " + data[HOST]);
			}
			
			String[] dateParts = data[START_DATE].split("/");
			/* Make sure we actually have a date here */
			if (dateParts.length > 1) {
				
				/* Date is in the form MM/DD/YYYY */
				String datePath = "/" + dateParts[2] + "/" + dateParts[0];
				
				Node importPathBaseNode = JcrUtils.getOrCreateByPath(targetPath, "sling:OrderedFolder", adminSession);
				Node importPathWithDate = JcrUtils.getOrCreateByPath(importPathBaseNode.getPath() + datePath, "sling:OrderedFolder", adminSession);
				
				/* Name of the page is of the format <EVENT TITLE>-MMM-dd
				 * (e.g. aem-basic-training-jan-01 */
				if (data[TITLE] != null && !data[TITLE].isEmpty()) {
					// 57 is 64 - (length of month string + length of day string + 2 hyphens)
					pageName = data[TITLE].substring(0, Math.min(data[TITLE].length(), 57));
					pageName = PageUtilities.EscapePageTitle(pageName);
				} else {
					pageName = DEFAULT_PAGE_NAME;
				}
				
				pageName = pageName + "-" + justTheMonth.format(startCal.getTime());
				pageName = pageName + "-" + justTheDay.format(startCal.getTime()); 
				
				/* If the page is in our uidMap, then it already exists and we can just update the existing page */
				if (uidMap != null && uidMap.containsKey(data[UID])) {
					
					eventPage = pageManager.getPage(uidMap.get(data[UID]));
					if (eventPage == null) {
						log.error(LOGGING_PREFIX + "Problem getting page for update " + uidMap.get(data[UID]));
						return;
					}
					
				} else {
					
					/* If it was not in the uidMap, then create a new page */
					eventPage = pageManager.create(importPathWithDate.getPath(), pageName, Constants.EVENT_TEMPLATE, data[TITLE]);
					
				}
				
				Node eventPageContentNode = eventPage.getContentResource().adaptTo(Node.class);
				/* Set the basic scaffolding and page type of the created page */
				eventPageContentNode.setProperty(PROP_SCAFFOLDING, EVENT_SCAFFOLDING_TEMPLATE);
				eventPageContentNode.setProperty(PROP_RESOURCE_TYPE, EVENT_COMPONENT_RES);
				/* Create the eventdetails node */
				Node eventDetailsNode = JcrUtils.getOrCreateByPath(eventPageContentNode.getPath() + "/" + EVENT_DETAILS_NODE, "nt:unstructured", adminSession);
				eventDetailsNode.setProperty(PN_EVENT_START, startCal);
				eventDetailsNode.setProperty(PN_EVENT_END, endCal);

				/* Start setting properties if they exist */
				String desc = null;
				if (!data[DESCRIPTION].trim().isEmpty()) {
					desc = appendSCCAButton(data[DESCRIPTION]);	
				} else {
					desc = appendSCCAButton("");
				}
				eventDetailsNode.setProperty(PN_EVENT_DESCRIPTION, desc);
				
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

				/* Add tags to the page */
				if (!tagList.isEmpty()) {

					Tag[] tags = tagList.toArray(new Tag[tagList.size()]);
					tagManager.setTags(eventPage.getContentResource(), tags);

				}
				
				/* Save everything */
				adminSession.save();
				
				/* If TrainingIsActive == false, then deactivate the page */
				if (eventPage != null && data[IS_ACTIVE].toLowerCase().equals("false")) {
					
					try {
						replicator.replicate(adminSession, ReplicationActionType.DEACTIVATE, eventPage.getPath());
					} catch (ReplicationException e) {
						log.error(LOGGING_PREFIX + "Problem deactivating page " + eventPage.getPath(), e);
					}

				}
				
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
			errorEmailService.sendEmail(COMMUNICATIONS_EMAIL_CONTACT, "LMS Importer error: Cannot replicate event", e.getMessage());
		} finally {

			/* Double-check that we are logged out */
			if (adminSession != null) {
				adminSession.logout();
			}
			
		}
		
	}
	
	/*
	 * Initializes a map of Cornerstone UIDs to AEM paths
	 */
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
	
	/*
	 * Initializes the tagMap of vendor titles to tag IDs
	 */
	private Map<String, String> createTagMap() {
		
		Map<String, String> map = new HashMap<String, String>();
		
		map.put(PN_VENDOR_CIT, PN_TAG_CIT);
		map.put(PN_VENDOR_CRS, PN_TAG_CRS);
		map.put(PN_VENDOR_COMMUNICATIONS, PN_TAG_COMMUNICATIONS);
		map.put(PN_VENDOR_EHS, PN_TAG_EHS);
		map.put(PN_VENDOR_FINANCIAL_PLANNING, PN_TAG_FINANCIAL_PLANNING);
		map.put(PN_VENDOR_FRED_HUTCH, PN_TAG_FRED_HUTCH);
		map.put(PN_VENDOR_FMIS, PN_TAG_FMIS);
		map.put(PN_VENDOR_HR_TRAINING, PN_TAG_HR_TRAINING);
		map.put(PN_VENDOR_RESEARCH_ETHICS, PN_TAG_RESEARCH_ETHICS);
		map.put(PN_VENDOR_IRO, PN_TAG_IRO);
		map.put(PN_VENDOR_OSR, PN_TAG_OSR);
		
		return map;
		
	}
	
	/*
	 * Turns a String array into a double-pipe-separated String to be passed to
	 * an error log or email.
	 * 
	 * @param String[] data - the array of data to be logged/emailed.
	 * 
	 * @returns String - a double-pipe-separated list of the data in the array
	 */
	private String createDataDump(String[] data) {
		
		StringBuffer sb = new StringBuffer();
		for (String s : data) {
			sb.append(s);
			sb.append("||");
		}
		
		return sb.toString();
		
	}
	
	/*
	 * Checks the tagMap to see if there is a vendor with the passed name and,
	 * if so, returns the department tag associated with that vendor. If there
	 * is no vendor or no tag associated with that vendor, returns null
	 * 
	 * @param String vendor - the name of the vendor. Should match one of the 
	 * constants listed above in the PN_VENDOR section.
	 * @param TagManager tagManager
	 * 
	 * @returns Tag - the department tag to place on this event
	 */
	private Tag getVendorTag(String vendor, TagManager tagManager) {
		
		Tag t;
		String tagID;
		
		if (tagMap.containsKey(vendor)) {
			
			tagID = tagMap.get(vendor);
			t = tagManager.resolve(tagID);
			
		} else {
			
			t = null;
			
		}
		
		return t;
		
	}
	
	/**
	 * Appends an HTML String that happens to be the code for a Button
	 * Component. See buttoncomponent.html in the Common CQ Package for the
	 * template this HTML was based on.
	 * @param s String representing the Event Description field of the imported
	 * event.
	 * @return the argument, s, with the code for a Button Component appended
	 * to the end of it.
	 */
	private String appendSCCAButton(String s) {

		StringBuffer sb = new StringBuffer();
		/* Null protection */
		if (s == null) {
			s = "";
		}
		
		/* Start with the original String */
		sb.append(s);
		/* Append the button HTML */
		sb.append("<p>");
		sb.append("<b>SCCA Employees:</b> to register for this course, ");
		sb.append("access Hutch Learning.");
		sb.append("</p>");
		sb.append("<div class=\"fh-component-button\">");
		sb.append("<a href=\"");
		/* This URL supplied by Sarah Koehn from HR Training */
		sb.append("https://auth.seattlecca.org/FHLMS/");
		sb.append("\">");
		/* The text visible inside the button */
		sb.append("Hutch Learning - SCCA");
		sb.append("</a>");
		sb.append("</div>");
		
		return sb.toString();
		
	}

}
