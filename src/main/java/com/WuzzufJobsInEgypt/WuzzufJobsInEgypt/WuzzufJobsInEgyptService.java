package com.WuzzufJobsInEgypt.WuzzufJobsInEgypt;

import org.knowm.xchart.SwingWrapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import smile.data.DataFrame;
import smile.data.measure.NominalScale;
import smile.data.vector.IntVector;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import java.io.IOException;
import java.util.*;

import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;

@RestController
public class WuzzufJobsInEgyptService {
    private Table WuzzufJobsTable;
    public  WuzzufJobsInEgyptService() throws IOException  {
        WuzzufJobsTable= Table.read().csv("D:/java/Wuzzuf_Jobs.csv");
        System.out.println("First 10 rows of the data:");
        System.out.println(WuzzufJobsTable.first(10));
    }

    //gets the summary of "Wuzzuf_Jobs.csv" in its string format
    @GetMapping("/get_summary")
    public String GetSummary(){
        return WuzzufJobsTable.summary().toString();
    }

    //gets the structure of "Wuzzuf_Jobs.csv" in its string format
    @GetMapping("/get_structure")
    public String GetStructure(){
        return WuzzufJobsTable.structure().toString();
    }

    //removes the duplicated and null rows from "Wuzzuf_Jobs.csv"
    // and gets the updated table in its string format
    @GetMapping("/clean_data")
    public String CleanData(){

        WuzzufJobsTable.dropDuplicateRows();
        WuzzufJobsTable.dropRowsWithMissingValues();

        return WuzzufJobsTable.toString();

    }

    //gets the most requiring companies for jobs and how many jobs it requires
    // in a string representation of TableSaw table format
    // with two columns "Category" which represents the company name
    //and "Count" for how many jobs it requires
    @GetMapping("/count_jobs_for_each_company")
    public String CountJobsForEachCompany(){
        Table count=WuzzufJobsTable.countBy("Company").sortDescendingOn("Count");
        return count.first(10).toString();

    }

    //gets the most repeated job titles
    // in a string representation of TableSaw table format
    // with two columns "Category" which represents the job title
    //and "Count" for how many times it has been repeated
    @GetMapping("/get_most_popular_job_titles")
    public String GetMostPopularJobTitles(){
        Table count=WuzzufJobsTable.countBy("Title").sortDescendingOn("Count").first(10);
        return count.toString();
    }

    //gets the most repeated locations of companies
    // in a string representation of TableSaw table format
    // with two columns "Category" which represents the company location
    //and "Count" for how many times it has been repeated
    @GetMapping("/get_most_popular_areas")
    public String GetMostPopularAreas(){
        Table count=WuzzufJobsTable.countBy("Location").sortDescendingOn("Count").first(10);
        return count.toString();
    }

    //gets the most required skills
    // in a string representation of TableSaw table format
    // with two columns "Category" which represents the skill required
    //and "Count" for how many times it has been required
    @GetMapping("/get_most_important_skills")
    public String GetSkills(){
        StringColumn col = WuzzufJobsTable.stringColumn("Skills");
        List<String> lst=col.asList();

        //converting the skills list to a map
        // where the key is the skill
        // and its value is the number of times this skill has been repeated in the list
        Map<String,Integer> skills=new HashMap<String,Integer>();
        for(int i=0;i<lst.size();i++){
            Arrays.stream((lst.get(i)).split(",")).forEach(a->{
                if(skills.containsKey(a)){
                    skills.put(a,skills.get(a)+1);
                }
                else{
                    skills.put(a,1);
                }
            });

        }
        Map<String, Integer> Skills=sortByValue(skills,false);
        Table table =
                Table.create("Skills")
                        .addColumns(
                                StringColumn.create("Skills", Skills.keySet()),
                                DoubleColumn.create("Count", Skills.values()));
        //System.out.println(table);
        return table.printAll();

    }

    //sorts a map input according to its values
    // ascending or descending through setting the parameter order to false or true respectively
    private Map<String, Integer> sortByValue(Map<String, Integer> map,boolean order)
    {
//convert HashMap into List
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
//sorting the list elements
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
            {
                if (order)
                {
//compare two object and return an integer
                    return o1.getValue().compareTo(o2.getValue());}
                else
                {
                    return o2.getValue().compareTo(o1.getValue());
                }
            }
        });
//prints the sorted HashMap
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        //System.out.println(sortedMap);
        return sortedMap;
    }

    @GetMapping("/factorize_YearsExp_column")
    public String FactorizeYearsExpColumn(){
        SmileConverter converter=new SmileConverter(WuzzufJobsTable);
        DataFrame df=converter.toDataFrame();
        String[] values = df.stringVector("YearsExp").distinct().toArray( new String[]{});
        int [] pclassValues = df.stringVector("YearsExp").factorize(new NominalScale(values)).toIntArray();
        df = df.merge(IntVector.of("Years", pclassValues));
        int[] years = df.intVector("Years").stream().toArray();
        IntColumn column = IntColumn.create("Years", years);
        WuzzufJobsTable=WuzzufJobsTable.addColumns(column);
        return WuzzufJobsTable.toString();
    }
}
