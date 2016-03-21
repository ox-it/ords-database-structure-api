/*
 * Copyright 2015 University of Oxford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ox.it.ords.api.database.structure.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.lifecycle.ResourceProvider;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.cxf.transport.local.LocalConduit;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.Factory;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureRoleService;
import uk.ac.ox.it.ords.api.database.structure.services.DatabaseStructureService;
import uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.api.database.structure.model.User;
import uk.ac.ox.it.ords.security.AbstractShiroTest;
import uk.ac.ox.it.ords.security.model.UserRole;

public class AbstractResourceTest extends AbstractShiroTest {

	protected final static String ENDPOINT_ADDRESS = "local://database-structure-api";
	protected static Server server;
	protected static HashMap<String, String> databaseIds;

	protected static void startServer() throws Exception {

	}
	
	public WebClient getClient(){
		List<Object> providers = new ArrayList<Object>();
		providers.add(new JacksonJsonProvider());
		WebClient client = WebClient.create(ENDPOINT_ADDRESS, providers);
		client.type("application/json");
		client.accept("application/json");
		WebClient.getConfig(client).getRequestContext().put(LocalConduit.DIRECT_DISPATCH, Boolean.TRUE);
		return client;
	}
	
	public static void createTestUsersAndRoles() throws Exception{
		
		DatabaseStructureService.Factory.getInstance().init();
		///
		/// I have no idea why this is needed to "kickstart" Hibernate
		/// into behaving properly for these tests. I *think* it binds
		/// the hibernate session to the same thread that is used by
		/// the resource classes, but I could be wrong. It doesn't seem
		/// to bother any of the other modules.
		/// 
		DatabaseStructureRoleService service = DatabaseStructureRoleService.Factory.getInstance();
		UserRole userRole = new UserRole();
		userRole.setPrincipalName("bob");
		userRole.setRole("viewer");
		service.addUserRoleToDatabase(1, userRole);	
		
		//
		// Set up the database
		//
		//
		// Set up the test users and their permissions
		//
		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();
		
		//
		// Clear out anything there already
		//
		session.createSQLQuery("truncate userrole, permissions, ordsuser, ordsphysicaldatabase").executeUpdate();
		transaction.commit();
		
		
		//
		// Add our test permissions
		//
		session = HibernateUtils.getSessionFactory().getCurrentSession();
		transaction = session.beginTransaction();
//		
//		//
//		// Anyone with the "User" role can contribute to projects
//		//
//		for (String permission : DatabaseStructurePermissionSets.getPermissionsForUser()){
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("user");
//			permissionObject.setPermission(permission);
//			session.save(permissionObject);
//		}
//		
//		//
//		// Anyone with the "LocalUser" role can create new trial projects
//		//
//		for (String permission : DatabaseStructurePermissionSets.getPermissionsForLocalUser()){
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("localuser");
//			permissionObject.setPermission(permission);
//			session.save(permissionObject);
//		}
//		
//		//
//		// Anyone with the "Administrator" role can create new full
//		// projects and upgrade projects to full, and update any
//		// user projects
//		//
//		for (String permission : DatabaseStructurePermissionSets.getPermissionsForSysadmin()){
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("administrator");
//			permissionObject.setPermission(permission);
//			session.save(permissionObject);
//		}
//
//		//
//		// "Anonymous" can View public projects
//		//
//		for (String permission : DatabaseStructurePermissionSets.getPermissionsForAnonymous()){
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("anonymous");
//			permissionObject.setPermission(permission);
//			session.save(permissionObject);
//		}
//	
		//
		// Add test users to roles
		//
		UserRole admin = new UserRole();
		admin.setPrincipalName("admin@nowhere.co");
		admin.setRole("administrator");
		session.save(admin);
		
		UserRole pingu = new UserRole();
		pingu.setPrincipalName("pingu@nowhere.co");
		pingu.setRole("localuser");
		session.save(pingu);
		
		UserRole pinga = new UserRole();
		pinga.setPrincipalName("pinga@penguins.com");
		pinga.setRole("user");
		session.save(pinga);
		
		UserRole anonymous = new UserRole();
		anonymous.setPrincipalName("anonymous@nowhere.co");
		anonymous.setRole("anonymous");
		session.save(anonymous);
		
		//
		// Create equivalent ords users
		//
		User adminOrds = new User();
		adminOrds.setName("admin");
		adminOrds.setPrincipalName("admin@nowhere.co");
		adminOrds.setEmail("admin@nowhere.co");
		adminOrds.setOdbcUser(adminOrds.getEmail().replace("@", "").replace(".", ""));
		adminOrds.setStatus(User.AccountStatus.VERIFIED.toString());
		adminOrds.setVerificationUuid(UUID.randomUUID().toString());
		session.save(adminOrds);

		User anonymousOrds = new User();
		anonymousOrds.setName("anonymous");
		anonymousOrds.setPrincipalName("anonymous@nowhere.co");
		anonymousOrds.setEmail("anonymous@nowhere.co");
		anonymousOrds.setOdbcUser(anonymousOrds.getEmail().replace("@", "").replace(".", ""));
		anonymousOrds.setStatus(User.AccountStatus.VERIFIED.toString());
		anonymousOrds.setVerificationUuid(UUID.randomUUID().toString());
		session.save(anonymousOrds);

		
		User pinguOrds = new User();
		pinguOrds.setName("pingu");
		pinguOrds.setPrincipalName("pingu@nowhere.co");
		pinguOrds.setEmail("pingu@nowhere.co");
		pinguOrds.setOdbcUser(pinguOrds.getEmail().replace("@", "").replace(".", ""));
		pinguOrds.setStatus(User.AccountStatus.VERIFIED.toString());
		pinguOrds.setVerificationUuid(UUID.randomUUID().toString());
		session.save(pinguOrds);
		
		User pingaOrds = new User();
		pingaOrds.setName("pinga");
		pingaOrds.setPrincipalName("pinga@penguins.com");
		pingaOrds.setEmail("pinga@penguins.com");
		pingaOrds.setOdbcUser(pingaOrds.getEmail().replace("@", "").replace(".", ""));
		pingaOrds.setStatus(User.AccountStatus.VERIFIED.toString());
		pingaOrds.setVerificationUuid(UUID.randomUUID().toString());
		session.save(pingaOrds);

		//
		// Commit our changes
		//
		transaction.commit();
		HibernateUtils.closeSession();
	}

	/**
	 * Configure Shiro and start the server
	 * @throws Exception
	 */
	@BeforeClass
	public static void initialize() throws Exception {
	
		//
		// Set up roles
		//
		createTestUsersAndRoles();
		
		//
		// This is for unit testing only and uses the test.shiro.ini configuration
		//
		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:test.shiro.ini");
		SecurityManager securityManager = factory.getInstance();
		SecurityUtils.setSecurityManager(securityManager);
		
		//
		// Create an embedded server with JSON processing
		//

		
		//
		// Create an embedded server with JSON processing
		//
		JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();
		
		ArrayList<Object> providers = new ArrayList<Object>();
		providers.add(new JacksonJsonProvider());
		//providers.add(new UnrecognizedPropertyExceptionMapper());
		//providers.add(new ValidationExceptionMapper());
		sf.setProviders(providers);
		
		//
		// Add our REST resources to the server
		//
		ArrayList<ResourceProvider> resources = new ArrayList<ResourceProvider>();
		
		//
		// Add our REST resources to the server
		//
		resources.add(new SingletonResourceProvider(new Database(), true));
		resources.add(new SingletonResourceProvider(new Odbc(), true));
		
		sf.setResourceProviders(resources);
		
		//
		// Start the server at the endpoint
		//
		sf.setAddress(ENDPOINT_ADDRESS);
		server = sf.create(); 
		databaseIds = new HashMap<String, String>();
		startServer();
	}

	@AfterClass
	public static void destroy() throws Exception {
		server.stop();
		server.destroy();
	}

	@After
	public void logout(){
		for ( String dbId: databaseIds.keySet() ) {
			WebClient client = getClient();
			String instance = databaseIds.get(dbId);
			Response r = client.path("/"+dbId+"/MAIN/"+instance+"/delete/false").delete();
		}
		SecurityUtils.getSubject().logout();
	}



}
