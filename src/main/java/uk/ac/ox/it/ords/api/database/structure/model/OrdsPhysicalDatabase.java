package uk.ac.ox.it.ords.api.database.structure.model;

import java.io.File;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;


@Entity
@Table(name = "ordsPhysicalDatabase")
public class OrdsPhysicalDatabase implements Cloneable {

    public enum EntityType {

        MAIN, TEST, MILESTONE;
    }

    public enum ImportType {

        QUEUED, SECONDARY_CSV_QUEUED, SECONDARY_CSV_IN_PROGRESS, IN_PROGRESS, FINISHED;
    }
    @Id
    @GeneratedValue
    private int physicalDatabaseId;
    private int logicalDatabaseId;
    private long fileSize;
    @Enumerated(EnumType.ORDINAL)
    private EntityType entityType;
    private String uploadedHost = null; // The host where the upload took place
    @NotNull
    private String fullPathToDirectory;
    @NotNull
    private String fileName;
    protected String databaseType;
    private int actorId;

    @Enumerated(EnumType.ORDINAL)
    private ImportType importProgress;

    /**
     * Does this entry exist as a real database in the server. TODO It may be
     * that this variable is now antiquated
     */
    private boolean representationExists;
    private boolean dbConsumed;
    @Column(name = "dbconsumedname", unique = true)
    private String dbConsumedName;
    
    private String databaseServer;
    
    @NotNull
    private String uuid;

    public OrdsPhysicalDatabase() {
        setUuid(UUID.randomUUID().toString());
        setImportProgress(ImportType.QUEUED);
    }

    public OrdsPhysicalDatabase clone() throws CloneNotSupportedException {
        return (OrdsPhysicalDatabase) super.clone();
    }

    /**
     * When a file has been uploaded to the server it is written to the file
     * system. However, several files might have the same name, so the actual
     * name of the file is calculated to be
     * &lt;storage folder&gt; / &lt;original filename&gt; _ uuid
     *
     * @return the calculated name showing where the file resides on disk
     */
    public String calculateFullNameOfFile() {
        String file = null;

        if (getEntityType() == null) {
            setEntityType(EntityType.MAIN);
        }

        if (getEntityType().equals(EntityType.MAIN)) {
            file = this.getFullPathToDirectory() + File.separator + this.getFileName() + "_" + this.getUuid();
        }
        else if (getEntityType().equals(EntityType.MILESTONE)) {
            file = calculateFullNameOfMilestoneFile();
        }
        else if (getEntityType().equals(EntityType.TEST)) {
            file = calculateFullNameOfTestFile();
        }

        return file;
    }

    public String calculateFullNameOfMilestoneFile() {
        String file = "";

        file = this.getFullPathToDirectory() + File.separator + this.getFileName() + "_Milestone_" + this.getUuid();

        return file;
    }

    public String calculateFullNameOfTestFile() {
        String file = "";

        file = this.getFullPathToDirectory() + File.separator + this.getFileName() + "_Test_" + this.getUuid();

        return file;
    }

    /**
     *
     * @return the size of the file as an easily displayable string
     */
    public String calculateDisplayableFileSize() {
        String readableFileSize;

        if (this.fileSize < 5000) {
            readableFileSize = String.format("%d bytes", this.fileSize);
        }
        else if (this.fileSize < 5000000) {
            readableFileSize = String.format("%d Kbytes", this.fileSize / 1024);
        }
        else {
            readableFileSize = String.format("%d Mbytes", this.fileSize / (1024 * 1024));
        }

        return readableFileSize;
    }

    public int getPhysicalDatabaseId() {
        return physicalDatabaseId;
    }

    public void setPhysicalDatabaseId(int physicalDatabaseId) {
        this.physicalDatabaseId = physicalDatabaseId;
    }

    public int getLogicalDatabaseId() {
        return logicalDatabaseId;
    }

    public void setLogicalDatabaseId(int logicalDatabaseId) {
        this.logicalDatabaseId = logicalDatabaseId;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getFullPathToDirectory() {
        return fullPathToDirectory;
    }

    public void setFullPathToDirectory(String fullPathToDirectory) {
        this.fullPathToDirectory = fullPathToDirectory;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long l) {
        this.fileSize = l;
    }

    public ImportType getImportProgress() {
        return importProgress;
    }

    public void setImportProgress(ImportType importProgress) {
        this.importProgress = importProgress;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public boolean isRepresentationExists() {
        return representationExists;
    }

    public void setRepresentationExists(boolean representationExists) {
        this.representationExists = representationExists;
    }

    public boolean isDbConsumed() {
        return dbConsumed;
    }

    public int getActorId() {
        return actorId;
    }

    public void setActorId(int actorId) {
        this.actorId = actorId;
    }

    /**
     * Get the PostgreSQL name of the database. This used to be stored in a table, but is
     * now automatically generated by this method using the properties of the database.
     * @return the name of the database
     */
    public String getDbConsumedName() {
          return (getEntityType().toString() + "_" + getPhysicalDatabaseId() + "_" + getLogicalDatabaseId()).toLowerCase();
    }

    public String getUploadedHost() {
        return uploadedHost;
    }

    public void setUploadedHost(String uploadedHost) {
        this.uploadedHost = uploadedHost;
    }

	public String getDatabaseServer() {
		return databaseServer;
	}

	public void setDatabaseServer(String databaseServer) {
		this.databaseServer = databaseServer;
	}
    
}
