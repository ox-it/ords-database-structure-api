package uk.ac.ox.it.ords.api.database.structure.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.hibernate.validator.constraints.Email;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@Table(name = "ordsuser")
public class User {
	
    private String principalName;
    private String principalType;
    private String odbcUser;
    
    @JsonIgnore 
    private String token;
    
    @Id
    @GeneratedValue
    private int userId;
    
    private String email;
    
    @NotNull
    @Size(min = 2, max = 200)
    private String name = "Unknown";
    
    public enum AccountStatus {PENDING_EMAIL_VERIFICATION, VERIFIED };
    private String status = AccountStatus.PENDING_EMAIL_VERIFICATION.toString();
    
    @JsonIgnore 
    private String verificationUuid;
    
    
    public static String ODBC_CALCULATED_NAME_SUFFIX = "_ords";

    
    public User() {
    	
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    @Email
    @Column(name = "email", unique = true)
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    @Column(name = "principalName", unique = true)
    public String getPrincipalName() {
        return principalName;
    }

    public void setPrincipalName(String principalName) {
        this.principalName = principalName;
    }

    public String getPrincipalType() {
        return principalType;
    }

    public void setPrincipalType(String principalType) {
        this.principalType = principalType;
    }
    
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @JsonIgnore 
    public String getVerificationUuid() {
        return verificationUuid;
    }

    @JsonIgnore 
    public void setVerificationUuid(String verificationUuid) {
        this.verificationUuid = verificationUuid;
    }
    
    
    public String getOdbcUser() {
		return odbcUser;
	}
    
    public String calculateOdbcUserForOrds() {
    	return getOdbcUser() + ODBC_CALCULATED_NAME_SUFFIX;
    }
    
    public static String getOdbcNameFromCalculatedName(String calculatedName) {
        if (calculatedName == null) {
            return null;
        }
        if (calculatedName.endsWith(ODBC_CALCULATED_NAME_SUFFIX)) {
            return calculatedName.substring(0, ODBC_CALCULATED_NAME_SUFFIX.length()-1);
        }
        return calculatedName;
    }


	public void setOdbcUser(String odbcUser) {
		this.odbcUser = odbcUser;
	}

    @JsonIgnore 
	public String getToken() {
		return token;
	}
    
    @JsonIgnore 
	public void setToken(String token) {
		this.token = token;
	}

}
