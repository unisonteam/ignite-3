package org.apache.ignite.internal.security;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Hashtable;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = @CreateTransport(protocol = "LDAP", address = "localhost", port = 10390, ssl = true))
@CreateDS(allowAnonAccess = false, partitions = @CreatePartition(name = "TestPartition", suffix = "dc=baeldung,dc=com"))
@ApplyLdifFiles("users.ldif")
public class LdapTest extends AbstractLdapTestUnit {
    private static void authenticateUser(Hashtable<String, String> environment) throws Exception {
        DirContext context = new InitialDirContext(environment);

        NamingEnumeration<SearchResult> search = context.search("ou=Users,dc=baeldung,dc=com", new BasicAttributes());
        search.asIterator().forEachRemaining(System.out::println);
        context.close();
    }

    @Test
    public void test() {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, "ldap://localhost:10390");
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");

        environment.put(Context.SECURITY_PRINCIPAL, "cn=Joe Simms,ou=Users,dc=baeldung,dc=com");
        environment.put(Context.SECURITY_CREDENTIALS, "12345");

        assertDoesNotThrow(() -> authenticateUser(environment));
    }

    @Test
    public void givenPreloadedLDAPUserJoe_whenAuthUserWithWrongPW_thenAuthFails() {

        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, "ldap://localhost:10390");
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");

        environment.put(Context.SECURITY_PRINCIPAL, "cn=Joe Simms,ou=Users,dc=baeldung,dc=com");
        environment.put(Context.SECURITY_CREDENTIALS, "wronguserpw");

        assertThrows(AuthenticationException.class, () -> authenticateUser(environment));
    }

    @Test
    public void givenPreloadedLDAPUserJoe_whenSearchAndAuthUserWithCorrectPW_thenAuthSucceeds() throws Exception {

        // first authenticate against LDAP as admin to search up DN of user : Joe Simms

        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, "ldap://localhost:10390");
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        environment.put(Context.SECURITY_CREDENTIALS, "secret");

        DirContext adminContext = new InitialDirContext(environment);

        // define the search filter to find the person with CN : Joe Simms
        String filter = "(&(objectClass=person)(cn=Joe Simms))";

        // declare the attributes we want returned for the object being searched
        String[] attrIDs = { "cn" };

        // define the search controls
        SearchControls searchControls = new SearchControls();
        searchControls.setReturningAttributes(attrIDs);
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        // search for User with filter cn=Joe Simms
        NamingEnumeration<SearchResult> searchResults = adminContext.search("dc=baeldung,dc=com", filter, searchControls);
        if (searchResults.hasMore()) {

            SearchResult result = searchResults.next();
            Attributes attrs = result.getAttributes();

            String distinguishedName = result.getNameInNamespace();
            assertThat(distinguishedName, is("cn=Joe Simms,ou=Users,dc=baeldung,dc=com"));

            String commonName = attrs.get("cn").toString();
            assertThat(commonName, is("cn: Joe Simms"));

            // authenticate new context with DN for user Joe Simms, using correct password

            environment.put(Context.SECURITY_PRINCIPAL, distinguishedName);
            environment.put(Context.SECURITY_CREDENTIALS, "12345");

            assertDoesNotThrow(() -> authenticateUser(environment));
        }

        adminContext.close();
    }

    @Test
    public void givenPreloadedLDAPUserJoe_whenSearchAndAuthUserWithWrongPW_thenAuthFails() throws Exception {

        // first authenticate against LDAP as admin to search up DN of user : Joe Simms

        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, "ldap://localhost:10390");
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        environment.put(Context.SECURITY_CREDENTIALS, "secret");
        DirContext adminContext = new InitialDirContext(environment);

        // define the search filter to find the person with CN : Joe Simms
        String filter = "(&(objectClass=person)(cn=Joe Simms))";

        // declare the attributes we want returned for the object being searched
        String[] attrIDs = { "cn" };

        // define the search controls
        SearchControls searchControls = new SearchControls();
        searchControls.setReturningAttributes(attrIDs);
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        // search for User with filter cn=Joe Simms
        NamingEnumeration<SearchResult> searchResults = adminContext.search("dc=baeldung,dc=com", filter, searchControls);
        if (searchResults.hasMore()) {

            SearchResult result = (SearchResult) searchResults.next();
            Attributes attrs = result.getAttributes();

            String distinguishedName = result.getNameInNamespace();
            assertThat(distinguishedName, is("cn=Joe Simms,ou=Users,dc=baeldung,dc=com"));

            String commonName = attrs.get("cn").toString();
            assertThat(commonName, is("cn: Joe Simms"));

            // authenticate new context with DN for user Joe Simms, using wrong password

            environment.put(Context.SECURITY_PRINCIPAL, distinguishedName);
            environment.put(Context.SECURITY_CREDENTIALS, "wronguserpassword");

            assertThrows(AuthenticationException.class, () -> authenticateUser(environment));
        }

        adminContext.close();
    }
}
