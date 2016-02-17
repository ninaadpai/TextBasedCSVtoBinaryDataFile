
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Map.Entry;
import java.util.*;
import com.csvreader.*;

public class MyDatabase {
	private static final String csvFile = "/Users/ninaad/Documents/MyJavaWork/JavaWorkspace/Project2/PHARMA_TRIALS_1000B.csv";
    private static final String IDIndexFile = "id.ndx";
    private static final String CompanyIndexFile = "company.ndx";
    private static final String DrugIDIndexFile = "drugid.ndx";
    private static final String TrialsIndexFile = "trials.ndx";
    private static final String PatientsIndexFile = "patients.ndx";
    private static final String DosageMGIndexFile = "dosage_mg.ndx";
    private static final String ReadingIndexFile = "reading.ndx";
    private static final String DoubleBlindIndexFile = "double_blind.ndx";
    private static final String ControlledStudyIndexFile = "controlled_study.ndx";
    private static final String GovtFundedIndexFile = "govt_funded.ndx";
    private static final String FDAApprovedIndexFile = "fda_approved.ndx";
    private static TreeMap<Integer, Long> idMap = new TreeMap<Integer, Long>();
    private static TreeMap<String, ArrayList<Long>> companyMap = new TreeMap<String, ArrayList<Long>>();
    private static TreeMap<String, ArrayList<Long>> drug_idMap = new TreeMap<String,ArrayList<Long>>();
    private static TreeMap<Integer, ArrayList<Long>> trialsMap = new TreeMap<Integer,ArrayList<Long>>();
    private static TreeMap<Integer, ArrayList<Long>> patientsMap = new TreeMap<Integer, ArrayList<Long>>();
    private static TreeMap<Integer, ArrayList<Long>> dosage_mgMap = new TreeMap<Integer, ArrayList<Long>>();
    private static TreeMap<Float, ArrayList<Long>> readingMap = new TreeMap<Float, ArrayList<Long>>();
    private static TreeMap<String, ArrayList<Long>> double_blindMap = new TreeMap<String, ArrayList<Long>>();
    private static TreeMap<String, ArrayList<Long>> controlled_studyMap = new TreeMap<String, ArrayList<Long>>();
    private static TreeMap<String, ArrayList<Long>> govt_fundedMap = new TreeMap<String, ArrayList<Long>>();
    private static TreeMap<String, ArrayList<Long>> fda_approvedMap = new TreeMap<String, ArrayList<Long>>();


public static void main(String args[]) throws IOException{
        clearfile("Data.db");
        clearfile("id.ndx");
        clearfile("company.ndx");	
        boolean flag;
        Scanner scanner = new Scanner(new InputStreamReader(System.in));
        System.out.println("Convert CSV to Binary?(y/N)");
        String input = scanner.nextLine();
        if (input.equalsIgnoreCase("y")) {
            flag = true;
        }
        else flag = false;

        CsvReader reader = new CsvReader(csvFile);
        ArrayList<Long> companyList;
        RandomAccessFile file = new RandomAccessFile("Data.db","rw");

        if (flag){
            System.out.println("Please wait... Index file are being created");
            reader.readHeaders();
            while (reader.readRecord())
            {
                int id = Integer.parseInt(reader.get("id"));
                String company = reader.get("company");
                String drug_id = reader.get("drug_id");
                int trials = Integer.parseInt (reader.get("trials"));
                int patients = Integer.parseInt(reader.get("patients"));
                int dosage_mg  = Integer.parseInt(reader.get("dosage_mg"));
                float reading     = Float.parseFloat(reader.get("reading"));
                String double_blind = reader.get("double_blind");
                String controlled_study =reader.get("controlled_study");
                String govt_funded = reader.get("govt_funded");
                String fda_approved = reader.get("fda_approved");

                long len =file.length();

                idMap.put(id, len);	
                file.writeInt(id);
                updateIdIndexFile(IDIndexFile);

                int val = 0;
                ArrayList<Long> trialList,drug_idList,patientsList,double_blindList,dosage_mgList,controlled_studyList, govt_fundedList, fda_approvedList,readingList;
                companyList = new ArrayList<Long>();
              
                if (companyMap.containsKey(company)) {
                        companyList = companyMap.get(company);
                }
                else {
                        companyList = new ArrayList<Long>();
                }
                companyList.add(len);
                companyMap.put(company, companyList);
                udpatecompanyIndexFile(CompanyIndexFile);
              
                if (trialsMap.containsKey(trials)) {
                    trialList = trialsMap.get(trials);
                }
                else {
                    trialList = new ArrayList<Long>();
                }
                trialList.add(len);             
                trialsMap.put(trials,trialList);
                udpatetrialsIndexFile(TrialsIndexFile);
                if (patientsMap.containsKey(patients)) {
                    patientsList = patientsMap.get(patients);
                    } else {
                            patientsList = new ArrayList<Long>();
                    }
                patientsList.add(len);
                patientsMap.put(patients,patientsList);
                udpatepatientsIndexFile(PatientsIndexFile);
                if (double_blindMap.containsKey(double_blind)) {
                    double_blindList = double_blindMap.get(double_blind);
                    } else {
                            double_blindList = new ArrayList<Long>();
                    }
                double_blindList.add(len);
               
                double_blindMap.put(double_blind,double_blindList);
                udpatedouble_blindIndexFile(DoubleBlindIndexFile);
                
                if (controlled_studyMap.containsKey(controlled_study)) {
                  controlled_studyList = controlled_studyMap.get(controlled_study);
                  } else {
                          controlled_studyList = new ArrayList<Long>();
                  }
                controlled_studyList.add(len);
               
                controlled_studyMap.put(controlled_study,controlled_studyList);
                udpatecontrolled_studyIndexFile(ControlledStudyIndexFile);
                
                if (govt_fundedMap.containsKey(govt_funded)) {
                    govt_fundedList = govt_fundedMap.get(govt_funded);
                    } else {
                            govt_fundedList = new ArrayList<Long>();
                    }
                govt_fundedList.add(len);
               
                govt_fundedMap.put(govt_funded,govt_fundedList);
                udpategovt_fundedIndexFile(GovtFundedIndexFile);
                
                if (fda_approvedMap.containsKey(fda_approved)) {
                    fda_approvedList = fda_approvedMap.get(fda_approved);
                    } else {
                            fda_approvedList = new ArrayList<Long>();
                    }
                fda_approvedList.add(len);
               
                fda_approvedMap.put(fda_approved,fda_approvedList);
                udpatefda_approvedIndexFile(FDAApprovedIndexFile);
                
                if (readingMap.containsKey(reading)) {
                    readingList = readingMap.get(reading);
                    } else {
                            readingList = new ArrayList<Long>();
                    }
                readingList.add(len);
                   
                readingMap.put(reading,readingList);
                udpatereadingIndexFile(ReadingIndexFile);
                
                
                if (dosage_mgMap.containsKey(dosage_mg)) {
                    dosage_mgList = dosage_mgMap.get(dosage_mg);
                    } else {
                            dosage_mgList = new ArrayList<Long>();
                    }
                dosage_mgList.add(len);
                   
                dosage_mgMap.put(dosage_mg,dosage_mgList);
                udpatedosage_mgIndexFile(DosageMGIndexFile);

                if (drug_idMap.containsKey(drug_id)) {
                    drug_idList = drug_idMap.get(drug_id);
                    } else {
                            drug_idList = new ArrayList<Long>();
                    }
                drug_idList.add(len);
                   
                drug_idMap.put(drug_id,drug_idList);
                udpatedrug_idIndexFile(DrugIDIndexFile);

                file.writeByte(company.length());
                file.writeBytes(company);
                file.writeBytes(drug_id);
                file.writeShort(trials);
                file.writeShort(patients);
                file.writeShort(dosage_mg);
                file.writeFloat(reading);
                
                val  = Boolean.parseBoolean(double_blind) ? val|8 : val;
                val =  Boolean.parseBoolean(controlled_study) ? val|4 : val;
                val =  Boolean.parseBoolean(govt_funded) ? val|2 : val;
                val =  Boolean.parseBoolean(fda_approved) ? val|1 : val; 
                file.writeByte(val); 

            }
        }
        String choice = "";
        do{
            
        System.out.println("\n1.id\n2.company\n3.drug_id\n4.trials\n5.patients\n6.dosage_mg\n7.reading\n8.double_blind\n9.controlled_study\n10.govt_funded\n11.fda_approved");
        System.out.println("Please select field (as given above)");
        String input3 = scanner.nextLine();
        if(!flag){
            System.out.println("Convert to binary first!");
        }
        else{
        String input1,input2,operator;
        switch(input3){
            case "id" :
                System.out.println("Enter id to be searched:");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                int id = Integer.parseInt(input1);
                operator = input2;
                
                search_Id(id, file,operator);
                break;


            case "drug_id" :
                System.out.println("Enter Drug ID");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                operator = input2;
                
                search_drug_id(input1, file,operator);
                break;
            case "trials" :
                System.out.println("Enter No. of Trials");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                int trials = Integer.parseInt(input1);
                operator = input2;
                
                search_trials(trials, file,operator,trialsMap);
                break;
            case "patients" :
                System.out.println("Enter No. of Patients");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                int patients = Integer.parseInt(input1);
                operator = input2;
                
                search_patients(patients, file,operator,patientsMap);
                break;
            case "company" :
                System.out.println("Enter Company Name");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");                
                operator = input2;
                
                search_company(input1, file,operator,companyMap);
                break;
            case "double_blind" :
                System.out.println("Enter Double Blind (true/false)");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_double_blind(input1, file,operator,double_blindMap);
                break;
            
            case "dosage_mg" :
                System.out.println("Enter Dosage(mG)");
                int dosage_mg = Integer.parseInt(scanner.nextLine());
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_dosage_mg(dosage_mg, file,operator,dosage_mgMap);
                break;
            
            case "reading" :
                System.out.println("Enter Reading");
                Float reading = Float.parseFloat(scanner.nextLine());
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_reading(reading, file,operator,readingMap);
                break;    
            
            case "controlled_study" :
                System.out.println("Enter Controlled Study (true/false)");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_controlled_study(input1, file,operator,controlled_studyMap);
                break;
            
            case "govt_funded" :
                System.out.println("Enter Government Funded (true/false)");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_govt_funded(input1, file,operator,govt_fundedMap);
                break;
            
            case "fda_approved" :
                System.out.println("Enter FDA Approved (true/false)");
                input1 = scanner.nextLine();
                System.out.println("Enter operator ");
                input2 = scanner.nextLine();
                System.out.println("---------------------------------");
                
                operator = input2;
                
                search_fda_approved(input1, file,operator,fda_approvedMap);
                break;
                
            default :
                System.out.println("Invalid choice, please try again");
                break;
        }
        System.out.println("Continue with another query?(y/N)");
        choice = scanner.nextLine();
    }
}while(choice.equalsIgnoreCase("y"));
}
		
private static void search_double_blind(String input1,RandomAccessFile file, String operator,TreeMap<String, ArrayList<Long>> double_blindMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("double_blind.ndx"));
    while ((sCurrentLine = br.readLine()) != null){
       
        String[]   element = sCurrentLine.split("\t");
    
        if (operator.equals("=")){
            if (element[0].equals(input1) ) {
                if (double_blindMap.containsKey(input1)) {
                    
                    ArrayList<Long> list = double_blindMap.get(input1);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        } else if (operator.equals("!=")){
            if (!element[0].equals(input1) ) {
                
                if (double_blindMap.containsKey(element[0])) {
                
                    ArrayList<Long> list = double_blindMap.get(element[0]);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        }
    }
    br.close();
}
private static void search_controlled_study(String input1,RandomAccessFile file, String operator,TreeMap<String, ArrayList<Long>> double_blindMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("controlled_study.ndx"));
    while ((sCurrentLine = br.readLine()) != null){
       
        String[]   element = sCurrentLine.split("\t");
    
        if (operator.equals("=")){
            if (element[0].equals(input1) ) {
               
                if (controlled_studyMap.containsKey(input1)) {
               
                    ArrayList<Long> list = controlled_studyMap.get(input1);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        } else if (operator.equals("!=")){
            if (!element[0].equals(input1) ) {
               
                if (controlled_studyMap.containsKey(element[0])) {
               
                    ArrayList<Long> list = controlled_studyMap.get(element[0]);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        }
    }
    br.close();
}
private static void search_govt_funded(String input1,RandomAccessFile file, String operator,TreeMap<String, ArrayList<Long>> double_blindMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("govt_funded.ndx"));
    while ((sCurrentLine = br.readLine()) != null){
       
        String[]   element = sCurrentLine.split("\t");
    
        if (operator.equals("=")){
            if (element[0].equals(input1) ) {
               
                if (govt_fundedMap.containsKey(input1)) {
                   
                    ArrayList<Long> list = govt_fundedMap.get(input1);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        } else if (operator.equals("!=")){
            if (!element[0].equals(input1) ) {
                
                if (govt_fundedMap.containsKey(element[0])) {
                   
                    ArrayList<Long> list = govt_fundedMap.get(element[0]);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        }
    }
    br.close();
}
private static void search_fda_approved(String input1,RandomAccessFile file, String operator,TreeMap<String, ArrayList<Long>> double_blindMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("fda_approved.ndx"));
    while ((sCurrentLine = br.readLine()) != null){
       
        String[]   element = sCurrentLine.split("\t");
    
        if (operator.equals("=")){
            if (element[0].equals(input1) ) {
                
                if (fda_approvedMap.containsKey(input1)) {
                    
                    ArrayList<Long> list = fda_approvedMap.get(input1);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        } else if (operator.equals("!=")){
            if (!element[0].equals(input1) ) {
                
                if (fda_approvedMap.containsKey(element[0])) {
                   
                    ArrayList<Long> list = fda_approvedMap.get(element[0]);
                    for (Long offset : list) {
                        displayRecords(file,offset,operator);
                    }
                }
            }
        }
    }
    br.close();
}
private static void search_patients(int patients, RandomAccessFile file,String operator, TreeMap<Integer, ArrayList<Long>> patientsMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("patients.ndx"));

    while ((sCurrentLine = br.readLine()) != null)  {
           

        String[]   element = sCurrentLine.split("\t");
    
        switch(operator){
            case "=":
                
         
            
                if (Integer.parseInt(element[0]) == patients) {
                 
                    if (patientsMap.containsKey(patients)) {
                 
                        ArrayList<Long> list = patientsMap.get(patients);
                        for (Long offset : list) {
                             displayRecords(file,offset,operator);
                            }
                    }

                }
                break;
                
            case ">=":
             if (Integer.parseInt(element[0]) > patients || Integer.parseInt(element[0])== patients){
                     if (patientsMap.containsKey(patients)) {
                            ArrayList<Long> list1 = patientsMap.get(Integer.parseInt(element[0]));
                            for (Long offset : list1) {
                             displayRecords(file,offset,operator);
                            }
                 
                     }
             }
        break;

            case ">":
                     if (Integer.parseInt(element[0]) > patients ){
                             if (patientsMap.containsKey(patients)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                 

                     }
                }
                break;
            case "<":
                     if (Integer.parseInt(element[0]) < patients ){
                             if (patientsMap.containsKey(patients)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                 

                     }
                }
                break;
            case "<=":
                     if (Integer.parseInt(element[0]) < patients || Integer.parseInt(element[0]) == patients){
                             if (patientsMap.containsKey(patients)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                 

                     }
                }
                break;
            case "!=":
                     if (Integer.parseInt(element[0]) != patients ){
                             if (patientsMap.containsKey(patients)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                 

                     }
                }
                break;
            default:
                System.out.println("invalid choice");
                break;
	}	
    }
    br.close();
}
private static void search_dosage_mg(int dosage_mg, RandomAccessFile file,String operator, TreeMap<Integer, ArrayList<Long>> patientsMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("dosage_mg.ndx"));

    while ((sCurrentLine = br.readLine()) != null)  {
            

        String[]   element = sCurrentLine.split("\t");
    
        switch(operator){
            case "=":
                
    
    
                if (Integer.parseInt(element[0]) == dosage_mg) {
    
                    if (patientsMap.containsKey(dosage_mg)) {
    
                        ArrayList<Long> list = patientsMap.get(dosage_mg);
                        for (Long offset : list) {
                             displayRecords(file,offset,operator);
                            }
                    }

                }
                break;
                
            case ">=":
             if (Integer.parseInt(element[0]) > dosage_mg || Integer.parseInt(element[0])== dosage_mg){
                     if (patientsMap.containsKey(dosage_mg)) {
                            ArrayList<Long> list1 = patientsMap.get(Integer.parseInt(element[0]));
                            for (Long offset : list1) {
                             displayRecords(file,offset,operator);
                            }
    
                     }
             }
        break;

            case ">":
                     if (Integer.parseInt(element[0]) > dosage_mg ){
                             if (patientsMap.containsKey(dosage_mg)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                             

                     }
                }
                break;
            case "<":
                     if (Integer.parseInt(element[0]) < dosage_mg ){
                             if (patientsMap.containsKey(dosage_mg)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                             

                     }
                }
                break;
            case "<=":
                     if (Integer.parseInt(element[0]) < dosage_mg || Integer.parseInt(element[0]) == dosage_mg){
                             if (patientsMap.containsKey(dosage_mg)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                             

                     }
                }
                break;
            case "!=":
                     if (Integer.parseInt(element[0]) != dosage_mg ){
                             if (patientsMap.containsKey(dosage_mg)) {
                                    ArrayList<Long> list2 = patientsMap.get(Integer.parseInt(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                             

                     }
                }
                break;
            default:
                System.out.println("invalid choice");
                break;
	}	
    }
    br.close();
}
private static void search_company(String company, RandomAccessFile file,String operator, TreeMap<String, ArrayList<Long>> companyMap) throws IOException {
		
		BufferedReader br = null;
		String sCurrentLine = "";
		br = new BufferedReader(new FileReader("company.ndx"));
		    switch(operator){
                        case "=" :
                            
                            while ((sCurrentLine = br.readLine()) != null){
                                String[]   element = sCurrentLine.split("\t");
                            if (element[0].equals(company) ){
		    		if (companyMap.containsKey(company)) {
		    			ArrayList<Long> list = companyMap.get(company);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
		    	}
                            }

                            break;
		    	case "!=" :
                            while ((sCurrentLine = br.readLine()) != null){
                                 String[] element = sCurrentLine.split("\t");
                                if (!element[0].equals(company) ){
                                   
                                    if (companyMap.containsKey(element[0])) {
		    			ArrayList<Long> list = companyMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
		    	}
                            }

                            break;    
                    
                         
                        case ">=":
                            String[] element;
                            while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                if (element[0].equals(company)){
                                 break;   
                                    
                                }
                                
                            }
                            
                               do{
                                 element = sCurrentLine.split("\t");

                                   
		    		if (companyMap.containsKey(element[0])) {
		    			ArrayList<Long> list = companyMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                               }while ((sCurrentLine = br.readLine()) != null);
		    	
                        break;
                            
                        case "<=":
                            
                               while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                   if (element[0].compareTo(company)<=0){
                                   element = sCurrentLine.split("\t");

                                   
		    		if (companyMap.containsKey(element[0])) {
		    			ArrayList<Long> list = companyMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                                
                                } else {
                                       break;
                                   }
                               }
		    	
                        break;
                        case "<":
                            
                               while ((sCurrentLine = br.readLine()) != null){
                                   element = sCurrentLine.split("\t");
                                   if (element[0].compareToIgnoreCase(company)<0){
                                      
                                   element = sCurrentLine.split("\t");

                                   
		    		if (companyMap.containsKey(element[0])) {
		    			ArrayList<Long> list = companyMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                                
                                } else {
                                       break;
                                   }
                               }
		    	
                        break;
                            
                        case ">":
                        
                            while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                if (element[0].equals(company)){
                                    sCurrentLine = br.readLine();
                                 break;   
                                    
                                }
                                
                            }
                            
                               do{
                                 element = sCurrentLine.split("\t");

                                   
		    		if (companyMap.containsKey(element[0])) {
		    			ArrayList<Long> list = companyMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                               }while ((sCurrentLine = br.readLine()) != null);
		    	
                        break;
                            
                        default: break;
			
			 }
		    
		    br.close();
		
	}
private static void search_trials(int trials, RandomAccessFile file,String operator, TreeMap<Integer, ArrayList<Long>> trialsMap) throws NumberFormatException, IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("trials.ndx"));

    while ((sCurrentLine = br.readLine()) != null)  {
        String[]   element = sCurrentLine.split("\t");
        switch(operator){


            case "=":
                if (Integer.parseInt(element[0]) == trials) {
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(trials);
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }
                    }

                }
                break;
            case "!=":
                if (Integer.parseInt(element[0]) != trials) {
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(trials);
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }
                    }

                }
                break;

            case ">=":
                if (Integer.parseInt(element[0]) >= trials ){
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(Integer.parseInt(element[0]));
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }

                    }
                }
                break;

            case ">":
                if (Integer.parseInt(element[0]) > trials ){
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(Integer.parseInt(element[0]));
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }
                    }
         
                }
                break;
            case "<=":
                if (Integer.parseInt(element[0]) <= trials ){
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(Integer.parseInt(element[0]));
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }
                    }
            
                }
                break;
            case "<":
                if (Integer.parseInt(element[0]) < trials ){
                    if (trialsMap.containsKey(trials)) {
                        ArrayList<Long> list = trialsMap.get(Integer.parseInt(element[0]));
                        for (Long offset : list) {
                            displayRecords(file,offset,operator);
                        }
                    }
            
                }
                break;
            default:
                System.out.println("invalid choice");
                break;
        }	
    }
    br.close();
}
private static void search_drug_id(String drug_id, RandomAccessFile file,String operator) throws NumberFormatException, IOException {
		
		BufferedReader br = null;
		String sCurrentLine = "";
		br = new BufferedReader(new FileReader("drugid.ndx"));
		    switch(operator){
                        case "=" :
                            
                            while ((sCurrentLine = br.readLine()) != null){
                                String[]   element = sCurrentLine.split("\t");
                            if (element[0].equals(drug_id) ){
		    		if (drug_idMap.containsKey(drug_id)) {
		    			ArrayList<Long> list = drug_idMap.get(drug_id);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
		    	}
                            }

                            break;
		    	case "!=" :
                            while ((sCurrentLine = br.readLine()) != null){
                                 String[] element = sCurrentLine.split("\t");
                                if (!element[0].equals(drug_id) ){
                                   
                                    if (drug_idMap.containsKey(element[0])) {
		    			ArrayList<Long> list = drug_idMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
		    	}
                            }

                            break;    
                    
                         
                        case ">=":
                            String[] element;
                            while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                if (element[0].equals(drug_id)){
                                 break;   
                                    
                                }
                                
                            }
                            
                               do{
                                 element = sCurrentLine.split("\t");

                                   
		    		if (drug_idMap.containsKey(element[0])) {
		    			ArrayList<Long> list = drug_idMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                               }while ((sCurrentLine = br.readLine()) != null);
		    	
                        break;
                            
                        case "<=":
                            
                               while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                   if (element[0].compareToIgnoreCase(drug_id)<=0){
                                   element = sCurrentLine.split("\t");

                                   
		    		if (drug_idMap.containsKey(element[0])) {
		    			ArrayList<Long> list = drug_idMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                                
                                } else {
                                       break;
                                   }
                               }
		    	
                        break;
                        case "<":
                            
                               while ((sCurrentLine = br.readLine()) != null){
                                   element = sCurrentLine.split("\t");
                                   if (element[0].compareToIgnoreCase(drug_id)<0){
                                      
                                   element = sCurrentLine.split("\t");
                                   
		    		if (drug_idMap.containsKey(element[0])) {
		    			ArrayList<Long> list = drug_idMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                                
                                } else {
                                       break;
                                   }
                               }
		    	
                        break;
                            
                        case ">":
                        
                            while ((sCurrentLine = br.readLine()) != null){
                                 element = sCurrentLine.split("\t");
                                if (element[0].equals(drug_id)){
                                    sCurrentLine = br.readLine();
                                 break;   
                                    
                                }
                                
                            }
                            
                               do{
                                 element = sCurrentLine.split("\t");
		    		if (drug_idMap.containsKey(element[0])) {
		    			ArrayList<Long> list = drug_idMap.get(element[0]);
		    			for (Long offset : list) {
					 displayRecords(file,offset,operator);
		    			}

		    		}
                               }while ((sCurrentLine = br.readLine()) != null);
		    	
                        break;
                            
                        default: break;
			
			 }
		    
		    br.close();
		
	}
private static void search_reading(float reading, RandomAccessFile file,String operator, TreeMap<Float, ArrayList<Long>> patientsMap) throws IOException {
    
    BufferedReader br = null;
    String sCurrentLine = "";
    br = new BufferedReader(new FileReader("reading.ndx"));

    while ((sCurrentLine = br.readLine()) != null)  {
        String[]   element = sCurrentLine.split("\t");
        switch(operator){
            case "=":
                if (Float.parseFloat(element[0]) == reading) {
                    if (readingMap.containsKey(reading)) {
                        ArrayList<Long> list = readingMap.get(reading);
                        for (Long offset : list) {
                             displayRecords(file,offset,operator);
                            }
                    }

                }
                break;
                
            case ">=":
             if (Float.parseFloat(element[0]) > reading || Float.parseFloat(element[0])== reading){
                     if (readingMap.containsKey(reading)) {
                            ArrayList<Long> list1 = readingMap.get(Float.parseFloat(element[0]));
                            for (Long offset : list1) {
                             displayRecords(file,offset,operator);
                            }
                     }
             }
        break;

            case ">":
                     if (Float.parseFloat(element[0]) > reading ){
                             if (readingMap.containsKey(reading)) {
                                    ArrayList<Long> list2 = readingMap.get(Float.parseFloat(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }

                     }
                }
                break;
            case "<":
                    
                     if (Float.parseFloat(element[0]) < reading ){
                             if (readingMap.containsKey(reading)) {
                                    ArrayList<Long> list2 = readingMap.get(Float.parseFloat(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }

                     }
                }
                break;
            case "<=":
                     if (Float.parseFloat(element[0]) < reading || Float.parseFloat(element[0]) == reading){
                             if (readingMap.containsKey(reading)) {
                                    ArrayList<Long> list2 = readingMap.get(Float.parseFloat(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }
                     }
                }
                break;
            case "!=":
                     if (Float.parseFloat(element[0]) != reading ){
                             if (readingMap.containsKey(reading)) {
                                    ArrayList<Long> list2 = readingMap.get(Float.parseFloat(element[0]));
                                    for (Long offset : list2) {
                                     displayRecords(file,offset,operator);
                                    }

                     }
                }
                break;
            default:
                System.out.println("invalid choice");
                break;
	}	
        br.close();
    }
}
private static void search_Id(int id, RandomAccessFile file,String operator) throws IOException {
        
        BufferedReader br = null;
        String sCurrentLine = "";
        br = new BufferedReader(new FileReader("id.ndx"));

        while ((sCurrentLine = br.readLine()) != null)  {
            String[]   element = sCurrentLine.split("\t");
            switch(operator){
                case "=":
                    if (Integer.parseInt(element[0]) == id ){
                                 displayRecords(file,Integer.parseInt(element[1]),operator);
                }
                break;

                case ">=":
                 if (Integer.parseInt(element[0]) > id || Integer.parseInt(element[0]) == id){
                         displayRecords(file,Integer.parseInt(element[1]),operator);

                 }
                break;
                case "<=":
                 if (Integer.parseInt(element[0]) < id || Integer.parseInt(element[0]) == id ){
                         displayRecords(file,Integer.parseInt(element[1]),operator);

                 }
                break;


                case ">":
                         if (Integer.parseInt(element[0]) > id ){
                                 displayRecords(file,Integer.parseInt(element[1]),operator);

                         }
                break;
                case "<":
                         if (Integer.parseInt(element[0]) < id ){
                                 displayRecords(file,Integer.parseInt(element[1]),operator);

                         }
                break;
                case "!=":
                 if (Integer.parseInt(element[0]) != id ){
                         displayRecords(file,Integer.parseInt(element[1]),operator);

                 }
                break;
                default:
                    System.out.println("Invalid Choice");
                    break;
            }
        }
        br.close();
    }

private static void displayRecords(RandomAccessFile file, long pointerPosition,String operator) throws IOException {
		
		    file.seek(pointerPosition);
		    int ID = file.readInt();
		   
		    int length=file.readByte();
			byte companyname []= new byte[length];
			byte drug_id []= new byte[6];
			 for (int i = 0 ; i < length; i++ ){
				  companyname[i] = file.readByte();
			}
			 String company = new String(companyname);
			 
			for (int i= 0 ; i <6 ; i++){
				drug_id[i] = file.readByte();	
			}
			String drug_id1 = new String(drug_id);
			
			int trials = file.readShort();
			int patients = file.readShort();
			int dosage_mg = file.readShort();
			float reading = file.readFloat();
			byte CommonByte=file.readByte();
			System.out.print(ID +" "+ company + " " + drug_id1 + " "+trials + " "+patients +" "+dosage_mg + " " +reading + "");
			System.out.print(" ");
			System.out.print(8==(byte)(CommonByte & 8));
			System.out.print(" ");
			System.out.print(4==(byte)(CommonByte & 4));
			System.out.print(" ");
			System.out.print(2==(byte)(CommonByte & 2));
			System.out.print(" ");
			System.out.print(1==(byte)(CommonByte & 1));
			System.out.println();
	}


private static void udpatecompanyIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<String, ArrayList<Long>>> nameSet = companyMap.entrySet();
		for (Entry<String, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
private static void udpatefda_approvedIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<String, ArrayList<Long>>> nameSet = fda_approvedMap.entrySet();
		for (Entry<String, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
private static void udpatedosage_mgIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<Integer, ArrayList<Long>>> nameSet = dosage_mgMap.entrySet();
		for (Entry<Integer, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
private static void updateIdIndexFile(String fileName) throws IOException {
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,false));
        Set<Entry<Integer, Long>> idSet = idMap.entrySet();
        for (Entry<Integer, Long> entry : idSet) {
                writer.write(entry.getKey() + "\t");
                writer.write(String.valueOf(entry.getValue()));
                writer.write("\n");
        }
        writer.close();
}
private static void udpatedrug_idIndexFile(String fileName) throws IOException {
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,false));
        Set<Entry<String, ArrayList<Long>>> idSet = drug_idMap.entrySet();
        for (Entry<String, ArrayList<Long>> entry : idSet) {


                        writer.write(entry.getKey() + "\t");
                        for (Long value : entry.getValue()) {
                                writer.write(String.valueOf(value) + "\t");
                        }
                        writer.write("\n");
                }
        writer.close();
}
private static void udpatedouble_blindIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<String, ArrayList<Long>>> nameSet = double_blindMap.entrySet();
		for (Entry<String, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();

	}
private static void udpatepatientsIndexFile(String fileName) throws IOException {
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,false));
        Set<Entry<Integer, ArrayList<Long>>> idSet = patientsMap.entrySet();
        for (Entry<Integer, ArrayList<Long>> entry : idSet) {
                        writer.write(entry.getKey() + "\t");
                        for (Long value : entry.getValue()) {
                        writer.write(String.valueOf(value) + "\t");
                        }
                        writer.write("\n");
                }
        writer.close();

    }
private static void udpatetrialsIndexFile(String fileName) throws IOException {
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,false));
    Set<Entry<Integer, ArrayList<Long>>> idSet = trialsMap.entrySet();
    for (Entry<Integer, ArrayList<Long>> entry : idSet) {
                    writer.write(entry.getKey() + "\t");
                    for (Long value : entry.getValue()) {
                    writer.write(String.valueOf(value) + "\t");
                    }
                    writer.write("\n");
            }
    writer.close();
}
private static void udpatereadingIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<Float, ArrayList<Long>>> nameSet = readingMap.entrySet();
		for (Entry<Float, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
private static void udpatecontrolled_studyIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<String, ArrayList<Long>>> nameSet = controlled_studyMap.entrySet();
		for (Entry<String, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
private static void udpategovt_fundedIndexFile(String fileName) throws IOException {
		
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName,false));
		Set<Entry<String, ArrayList<Long>>> nameSet = govt_fundedMap.entrySet();
		for (Entry<String, ArrayList<Long>> listEntry : nameSet) {
			fileWriter.write(listEntry.getKey() + "\t");
			for (Long value : listEntry.getValue()) {
				fileWriter.write(String.valueOf(value) + "\t");
			}
			fileWriter.write("\n");
		}
		fileWriter.close();
	}
public void loadIDIndexMap(String fileName) throws IOException {
    BufferedReader fileReader = new BufferedReader(new FileReader(fileName));
    idMap = new TreeMap<Integer, Long>();
    String line = null;
    if (fileReader != null) {
            while ((line = fileReader.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    idMap.put(Integer.parseInt(tokens[0]),
                                    Long.parseLong(tokens[1]));
            }
    }
    fileReader.close();

}

private static void clearfile(String filename) throws IOException {
    
    FileWriter fw = new FileWriter(filename);
    PrintWriter pw = new PrintWriter(fw);
    pw.write("");
    pw.flush(); 
    pw.close();
}
	
	
}