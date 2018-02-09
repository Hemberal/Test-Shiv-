import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

class SearchData {
	
	public static void main(String... s) throws Exception {
		
		Class.forName("oracle.jdbc.driver.OracleDriver");	
		
		Connection connection = null;

        try {

            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@odb-npd-2.canonhosted.net:1542/CPQSD1", "KKNKAPPSERVER", "KKNKAPPSERVER");
            
            
            ResultSet allTables = connection.getMetaData().getTables(null, "KKNKAPPSERVER", null, new String[] {"TABLE"});
            
            ArrayList<String> tables = new ArrayList<String>();
            
            while(allTables.next()) {
            	
            	for ( int i=1; i<=allTables.getMetaData().getColumnCount(); i++) {
            		
            		//System.out.println(allTables.getMetaData().getColumnName(i)+" : "+allTables.getString(i));
            		
            		if(allTables.getMetaData().getColumnName(i).equalsIgnoreCase("TABLE_NAME")) {
            			tables.add(allTables.getString(i));
            			
            		}
            		
            	}
            }
            
            //System.out.println(tables);
            
            String searchStr = "Enable/Disable CreditCheck workflow at NSO level";//"AccrualType.Normal";//;
            
            
            for(String tName : tables) {
            	if(tName.equals("AT_NODE")) {
            		System.out.println("-----Searching in table : "+tName+" ...");
                	
                	ResultSet tableRows = connection.createStatement().executeQuery("select * from "+tName);
                	
                	ResultSet rsPKDetails = connection.getMetaData().getPrimaryKeys(null, "KKNKAPPSERVER", tName);
                	ArrayList<String> pkCols = new ArrayList<String>();
                	while(rsPKDetails.next()) {
                		pkCols.add( rsPKDetails.getString(4) );
                	}
                	//System.out.println(pkCols);
                	
                	while(tableRows.next()) {
                		
                		HashMap<String, String> match = new HashMap<String, String>();
                		
                		for(int i=1; i<=tableRows.getMetaData().getColumnCount(); i++) { 
                			if(tableRows.getString(i)!=null && tableRows.getString(i).contains( searchStr  )) {
                				
                				match.put(tableRows.getMetaData().getColumnName(i), tableRows.getString(i));            				
                			}            			
                		}
                		
                		
                		
                		if(!match.isEmpty()) {
                			String pkDetails = "";
                			for(String pkC : pkCols) {
                				pkDetails = pkDetails + pkC+"="+tableRows.getString(pkC) +", ";
                			}
                			
                			System.out.println("\tMatches : "+pkDetails+" >> "+match);
                		}
                		
                		
                	}
            	}
            	
            	
            }
            
            
            

        } catch (Exception e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        } finally {
			try {
				connection.close();
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
	
}