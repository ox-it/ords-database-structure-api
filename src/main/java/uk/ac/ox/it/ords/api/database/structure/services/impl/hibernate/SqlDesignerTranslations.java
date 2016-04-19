package uk.ac.ox.it.ords.api.database.structure.services.impl.hibernate;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class to help map native Postgres datatypes to those needed by the sql designer
 * @author oucs0153
 *
 */
public class SqlDesignerTranslations {
	private static Logger log = LoggerFactory.getLogger(SqlDesignerTranslations.class);

    private static final String[] lengthTypes = {"CHARACTER VARYING", 
                                                "CHARACTER", 
                                                "TIME", 
                                                "TIMESTAMP WITHOUT TIME ZONE", 
                                                "NUMERIC"};

	public static String[][] datatypeMappings = { {"CHARACTER VARYING", "VARCHAR"},
        {"CHARACTER", "CHAR"},
		{"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP"},
		{"BINARY", "BOOLEAN"},
        {"NUMERIC", "DECIMAL"}};

    


	public static boolean isVarCharDataType(String input) {
		return (input.compareToIgnoreCase("CHARACTER VARYING") == 0);
	}

	/**
     * Postgres has its own idea of what a datatype looks like and this can be different from what we
     * have in the input data. This routine will attempt to convert between datatypes from other databases and
     * Postgres.
     * See http://en.wikibooks.org/wiki/Converting_MySQL_to_PostgreSQL for information on some of
     * the conversions possible
     *
     * @param dataType the datatype to be converted
     * TODO merge with other type translation utilities
     * @return a Postgres-friendly datatype
     */
	public static String convertDatatypeForPostgres(String dataType) {
        String returnedDataType = dataType;

        if (dataType.compareToIgnoreCase("mediumtext") == 0) {
            returnedDataType = "text";
        }
        else if (dataType.compareToIgnoreCase("binary") == 0) {
            returnedDataType = "boolean";//"bytea";
        }
        else if (dataType.toLowerCase().contains("varchar")) {
            returnedDataType = "character varying";
            if (dataType.contains("(")) {
            	returnedDataType += dataType.substring(dataType.indexOf("("));
            }
        }

        return returnedDataType;
    }


	public static String translateDatatype(String input, String fieldSize) {
		String output = input;

		for (String[] s : datatypeMappings) {
			if (input.toUpperCase().equals(s[0])) {
				if (Arrays.asList(lengthTypes).contains(input)) {
					output = String.format("%s(%s)", s[1], fieldSize);
				}
				else {
					output = s[1];
				}
				break;
			}
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format("Have just translated <%s> to <%s>", input, output));
		}

		return output;
	}
}
