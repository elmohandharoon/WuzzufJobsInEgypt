package com.WuzzufJobsInEgypt.WuzzufJobsInEgypt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WuzzufJobsInEgyptApplication {

	public static void main(String[] args) {

		System.setProperty("java.awt.headless", "false");
		SpringApplication.run(WuzzufJobsInEgyptApplication.class, args);
		//requirement 1
		WuzzufJobsInEgyptClient client=new WuzzufJobsInEgyptClient();

		/*
		/////UNCOMMENT THE REQUIREMENT TO TEST ITS RESULT
		*/

		//requirement 2
		//client.DisplayStructureAndSummary();
		//requirement 3
		//client.CleanData();
		//requirement 4
		//client.ShowCountOfJobsForEachCompanyInAPieChart();
		//requirement 5
		//client.CountJobsForEachCompany();
		//requirement 6
		//client.GetMostPopularJobTitles();
		//requirement 7
		//client.ShowMostPopularJobTitlesInABarChart();
		//requirement 8
		//client.GetMostPopularAreas();
		//requirement 9
		//client.ShowMostPopularAreasInABarChart();
		//requirement 10
		//client.GetMostImportantSkills();
		//requirement 11
		//client.FactorizeYearsExpColumn();
	}

}
