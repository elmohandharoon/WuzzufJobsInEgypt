package com.WuzzufJobsInEgypt.WuzzufJobsInEgypt;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;




import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.columns.Column;
import tech.tablesaw.table.Relation;

//this class consumes the service "WuzzufJobsInEgyptService"
//and prints the data coming with the response on console
public class WuzzufJobsInEgyptClient {
    URL Url;
    public WuzzufJobsInEgyptClient(){
        try {
            Url = new URL("http://localhost:8081/");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public void DisplayStructureAndSummary(){
        ArrayList<String> br=getDataFromConnection(Url+"get_structure");
        Table structureTable= ConvertToStructureTable(br);
        System.out.println("Wuzzuf_Jobs Structure: ");
        System.out.println(structureTable);

        System.out.println();
        System.out.println();


        ArrayList<String> br2=getDataFromConnection(Url+"get_summary");
        Table summaryTable= ConvertToSummaryTable(br2);
        System.out.println("Wuzzuf_Jobs Summary: ");
        System.out.println(summaryTable);

    }
    public void CleanData(){
        ArrayList<String> br=getDataFromConnection(Url+"clean_data");
        Table wuzzufTable= ConvertToWuzzufTable(br);
        System.out.println("Data are clean: ");
        System.out.println("New Clean data: ");
        System.out.println(wuzzufTable);

    }
    public void ShowCountOfJobsForEachCompanyInAPieChart(){
        Table DemandingCompanies=CountJobsForEachCompany();
        drawPieChart(DemandingCompanies,"Count of jobs per Company","Company");
    }
    public Table CountJobsForEachCompany(){
        ArrayList<String> br=getDataFromConnection(Url+"count_jobs_for_each_company");

        Table DemandingCompanies= ConvertToTable(br,"Most Demanding Companies for jobs","Company","Count");
        System.out.println(DemandingCompanies);

        return DemandingCompanies;
    }
    public Table GetMostPopularJobTitles(){
        ArrayList<String> br=getDataFromConnection(Url+"get_most_popular_job_titles");

        Table JobTitles = ConvertToTable(br,"Most Popular Job Titles","Title","Count");
        System.out.println(JobTitles);

        return JobTitles;
    }
    public void ShowMostPopularJobTitlesInABarChart(){
        Table JobTitles =GetMostPopularJobTitles();
        drawBarChart(JobTitles,"Number of job titles per Company","Title");
    }
    public Table GetMostPopularAreas(){
        ArrayList<String> br=getDataFromConnection(Url+"get_most_popular_areas");

        Table Locations = ConvertToTable(br,"Most Popular Job Titles","Location","Count");
        System.out.println(Locations);

        return Locations;
    }
    public void ShowMostPopularAreasInABarChart(){
        Table JobTitles =GetMostPopularAreas();
        drawBarChart(JobTitles,"Most Popular Locations","Location");
    }
    public void GetMostImportantSkills(){
        ArrayList<String> br=getDataFromConnection(Url+"get_most_important_skills");

        Table Skills= ConvertToTable(br,"Most Important Skills Required","Skills","Count");
        System.out.println(Skills);
    }
    public void FactorizeYearsExpColumn(){
        ArrayList<String> br=getDataFromConnection(Url+"factorize_YearsExp_column");
        Table wuzzufTable= ConvertToWuzzufTable(br);
        System.out.println("New data after factorizing YearsExp column: ");
        System.out.println(wuzzufTable);

    }



    //a method to convert a list of string to a table of 2 columns
    //a category column(which you can specify its name using "col1" parameter)
    //and a column to record how many times this category has been repeated throwout its original table
    private Table ConvertToTable(ArrayList<String> br,String tableName,String col1,String col2){
        String output;
        Table table=null;


        ArrayList<String> Category = new ArrayList<String>();
        ArrayList<Integer> Count = new ArrayList<Integer>();
        String[] values;
        int i=3;
        for (i=3;i<br.size();i++) {
            //System.out.println((br.get(i)));
            values = br.get(i).split("\\|");
            Category.add(values[0]);
            Count.add(Integer.parseInt(values[1].trim()));
            table =
                    Table.create(tableName)
                            .addColumns(
                                    StringColumn.create(col1, Category),
                                    DoubleColumn.create(col2, Count));


        }

        return table;
    }

    //converts any string representation of a summary table back to its table format
    private Table ConvertToSummaryTable(ArrayList<String> br){
        String output;
        Table table=null;


        ArrayList<String> Summary = new ArrayList<String>();
        ArrayList<String> Title = new ArrayList<String>();
        ArrayList<String> Company = new ArrayList<String>();
        ArrayList<String> Location = new ArrayList<String>();
        ArrayList<String> Type = new ArrayList<String>();
        ArrayList<String> Level = new ArrayList<String>();
        ArrayList<String> YearsExp = new ArrayList<String>();
        ArrayList<String> Country = new ArrayList<String>();
        ArrayList<String> Skills = new ArrayList<String>();

        String[] values;
        int i=3;
        for (i=3;i<br.size();i++) {

            values = br.get(i).split("\\|");
            Summary.add(values[0]);
            Title.add(values[1]);
            Company.add(values[2]);
            Location.add(values[3]);
            Type.add(values[4]);
            Level.add(values[5]);
            YearsExp.add(values[6]);
            Country.add(values[7]);
            Skills.add(values[8]);




            table =
                    Table.create("Wuzzuf_Jobs.csv")
                            .addColumns(
                                    StringColumn.create("Summary", Summary),
                                    StringColumn.create("Title", Title),
                                    StringColumn.create("Company", Company),
                                    StringColumn.create("Location", Location),
                                    StringColumn.create("Type", Type),
                                    StringColumn.create("Level", Level),
                                    StringColumn.create("YearsExp", YearsExp),
                                    StringColumn.create("Country", Country),
                                    StringColumn.create("Skills", Skills)

                            );


        }

        return table;
    }

    //converts any string representation of a "Wuzzuf_Jobs.csv" table back to its table format
    private Table ConvertToWuzzufTable(ArrayList<String> br){
        String output;
        Table table=null;


        ArrayList<String> Title = new ArrayList<String>();
        ArrayList<String> Company = new ArrayList<String>();
        ArrayList<String> Location = new ArrayList<String>();
        ArrayList<String> Type = new ArrayList<String>();
        ArrayList<String> Level = new ArrayList<String>();
        ArrayList<String> YearsExp = new ArrayList<String>();
        ArrayList<String> Country = new ArrayList<String>();
        ArrayList<String> Skills = new ArrayList<String>();
        ArrayList<String> Years = new ArrayList<String>();


        String[] values;
        int i=3;
        for (i=3;i<br.size();i++) {

            values = br.get(i).split("\\|");
            Title.add(values[0]);
            Company.add(values[1]);
            Location.add(values[2]);
            Type.add(values[3]);
            Level.add(values[4]);
            YearsExp.add(values[5]);
            Country.add(values[6]);
            Skills.add(values[7]);
            if(values.length==9)
                Years.add(values[8]);


            if(values.length!=9) {
                table =
                        Table.create("Wuzzuf_Jobs.csv")
                                .addColumns(
                                        StringColumn.create("Title", Title),
                                        StringColumn.create("Company", Company),
                                        StringColumn.create("Location", Location),
                                        StringColumn.create("Type", Type),
                                        StringColumn.create("Level", Level),
                                        StringColumn.create("YearsExp", YearsExp),
                                        StringColumn.create("Country", Country),
                                        StringColumn.create("Skills", Skills)
                                );
                }
            else {

                table =
                        Table.create("Wuzzuf_Jobs.csv")
                                .addColumns(
                                        StringColumn.create("Title", Title),
                                        StringColumn.create("Company", Company),
                                        StringColumn.create("Location", Location),
                                        StringColumn.create("Type", Type),
                                        StringColumn.create("Level", Level),
                                        StringColumn.create("YearsExp", YearsExp),
                                        StringColumn.create("Country", Country),
                                        StringColumn.create("Skills", Skills),
                                        StringColumn.create("Years", Years)
                                );
            }


        }

        return table;
    }

    //converts any string representation of a structure table back to its table format
    private Table ConvertToStructureTable(ArrayList<String> br){
        String output;
        Table table=null;


        ArrayList<String> Index = new ArrayList<String>();
        ArrayList<String> ColumnName = new ArrayList<String>();
        ArrayList<String> ColumnType = new ArrayList<String>();


        String[] values;
        int i=3;
        for (i=3;i<br.size();i++) {
            //System.out.println((br.get(i)));
            values = br.get(i).split("\\|");
            Index.add(values[0]);
            ColumnName.add(values[1]);
            ColumnType.add(values[2]);



            table =
                    Table.create("Structure of Wuzzuf_Jobs.csv")
                            .addColumns(
                                    StringColumn.create("Index", Index),
                                    StringColumn.create("Column Name", ColumnName),
                                    StringColumn.create("Column Type", ColumnType)

                            );


        }

        return table;
    }

    //draws a piechart for any table containing 2 columns
    //the first column represents the category(which you can specify its name through "colName" parameter)
    //and the second column must be of name "Count" and of type "int"
    private void drawPieChart(Table table,String title,String colName){
        PieChart chart =new PieChartBuilder().width(800).height(600).title(title).build();
        for (int i =0;i<table.rowCount();i++){
            chart.addSeries(table.getString(i, colName), (Number) table.get(i, 1));
        }
        new SwingWrapper(chart).displayChart();

    }

    //draws a barchart for any table containing 2 columns
    //the first column represents the category(which you can specify its name through "colName" parameter)
    //and the second column must be of name "Count" and of type "int"
    private void drawBarChart(Table table,String title,String colName){
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(colName).yAxisTitle("Count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries(title,table.column(colName).asList(), (List<? extends Number>) table.column("Count").asList());
        new SwingWrapper(chart).displayChart();
    }

    //reads the response out of "WuzzufJobsInEgyptService" service through its corresponding url
    //and convert it to a list of strings representation
    // where each item in this list represents a string representation of a row in some table
    // (as "WuzzufJobsInEgyptService" service always returns a TableSaw table in its string format)
    private ArrayList<String> getDataFromConnection(String URl){
        BufferedReader br=null;
        ArrayList<String> data=new ArrayList<String>();
        try {
            URL url = new URL(URl);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP Error code : "
                        + conn.getResponseCode());
            }
            InputStreamReader in = new InputStreamReader(conn.getInputStream());
            br = new BufferedReader(in);
            String output;
            while ((output = br.readLine()) != null) {
                data.add((output));
            }
            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}
