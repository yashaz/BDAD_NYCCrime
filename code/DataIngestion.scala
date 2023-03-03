import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat

object DataIngestion {

    def getTs(dateStr: String): String = {
        val inputStr = new SimpleDateFormat("hh:mm:ssaa")
        val outputStr = new SimpleDateFormat("HH:mm:ss")
        val timeStr = dateStr.split(" ")(1) + dateStr.split(" ")(2)
        val timeTS = outputStr.format(inputStr.parse(timeStr))
        timeTS
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder().getOrCreate()
        val nyc911CallsfileName = "/user/yc4953_nyu_edu/project/NYPD_Calls_for_Service__Historic_.csv"
        val nyc911callsOriginal = spark.read.format("csv").option("header","true").option("inferSchema","true").load(nyc911CallsfileName)
        val nyc911callsExtracted = nyc911callsOriginal.select("CAD_EVNT_ID","CREATE_DATE","INCIDENT_DATE","INCIDENT_TIME","NYPD_PCT_CD","BORO_NM","PATRL_BORO_NM","RADIO_CODE","TYP_DESC","CIP_JOBS","ADD_TS","DISP_TS","ARRIVD_TS","CLOSNG_TS","Latitude","Longitude")
        val nyc911callsNADrop = nyc911callsExtracted.na.drop(Seq("ARRIVD_TS","NYPD_PCT_CD","DISP_TS","CLOSNG_TS"))
        val nyc911callsFiltered = nyc911callsNADrop.filter(!(col("BORO_NM").equalTo("(null)"))).filter(!(col("PATRL_BORO_NM").equalTo("(null)"))).filter(!(col("TYP_DESC").equalTo("(null)")))
        val transformTs = udf((x : String) => getTs(x))
        val nyc911callstransformedTS = nyc911callsFiltered.withColumn("ADD_TS_TRANSFORMED", transformTs(col("ADD_TS"))).
                                             withColumn("DISP_TS_TRANSFORMED", transformTs(col("DISP_TS"))).
                                             withColumn("ARRIVD_TS_TRANSFORMED", transformTs(col("ARRIVD_TS"))).
                                             withColumn("CLOSNG_TS_TRANSFORMED", transformTs(col("CLOSNG_TS")))
        val nyc911callsFinal = nyc911callstransformedTS.select("CAD_EVNT_ID","CREATE_DATE","INCIDENT_DATE","INCIDENT_TIME","NYPD_PCT_CD","BORO_NM","PATRL_BORO_NM","RADIO_CODE","TYP_DESC","CIP_JOBS","ADD_TS_TRANSFORMED","DISP_TS_TRANSFORMED","ARRIVD_TS_TRANSFORMED","CLOSNG_TS_TRANSFORMED","Latitude","Longitude")
        nyc911callsFinal.write.option("header",true).format("csv").save("/user/yc4953_nyu_edu/project/NYC911CallsDataFinal")


        val nycHateCrimesfileName = "/user/yc4953_nyu_edu/project/NYPD_Hate_Crimes.csv"
        val nycHateCrimesOriginal = spark.read.format("csv").option("header","true").option("inferSchema","true").load(nycHateCrimesfileName)
        val nycHateCrimesExtracted = nycHateCrimesOriginal.select("Full Complaint ID","Record Create Date","Complaint Precinct Code","Patrol Borough Name","County","Law Code Category Description","Offense Description","PD Code Description","Bias Motive Description","Offense Category","Arrest Date")
        val nycHateCrimesFinal = nycHateCrimesExtracted.na.fill("No Arrests", Seq("Arrest Date"))
        nycHateCrimesFinal.write.option("header",true).format("csv").save("/user/yc4953_nyu_edu/project/HateCrimeDataFinal")

        val arrestDataPath = "/user/lr2947_nyu_edu/project/NYPD_Arrests_Data_Historic.csv"
        val rawArrestData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(arrestDataPath)
        val arrestData = rawArrestData.select("ARREST_DATE","OFNS_DESC", "ARREST_BORO", "ARREST_PRECINCT", "JURISDICTION_CODE", "AGE_GROUP", "PERP_SEX","PERP_RACE",  "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude", "Lon_Lat" )
        val offenceDespUpdatedData = arrestData.na.fill("Not Known", Array("OFNS_DESC"))
        val filterArrestData = offenceDespUpdatedData.na.drop(Seq("ARREST_DATE","OFNS_DESC", "ARREST_BORO", "ARREST_PRECINCT", "JURISDICTION_CODE", "AGE_GROUP", "PERP_SEX","PERP_RACE",  "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude", "Lon_Lat" ))
        val filterArrestData1 = filterArrestData.select("*").where(filterArrestData("AGE_GROUP") === "<18" || filterArrestData("AGE_GROUP") === "18-24" || filterArrestData("AGE_GROUP") === "25-44" || filterArrestData("AGE_GROUP") === "45-64" || filterArrestData("AGE_GROUP") === "65+" )
        filterArrestData1.write.format("parquet").save("/user/lr2947_nyu_edu/project/Cleaned_Arrest.parquet")


        val populationDataPath = "/user/lr2947_nyu_edu/project/New_York_City_Population_by_Borough_1950-_2040.csv"
        val rawPopulationData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(populationDataPath)
        val populationData = rawPopulationData.select(rawPopulationData("Age Group").alias("Age_group"), rawPopulationData("Borough"), rawPopulationData("1950"),rawPopulationData("1960"),rawPopulationData("1970"),rawPopulationData("1980"),rawPopulationData("1990"),rawPopulationData("2000"),rawPopulationData("2010"),rawPopulationData("2020"),rawPopulationData("2030"),rawPopulationData("2040"))
        populationData.write.format("parquet").save("/user/lr2947_nyu_edu/project/Cleaned_population.parquet")

        val complaintDataPath= "/user/ss14431_nyu_edu/bdadProject/orignalDataSet/NYPD_Complaint_Data_Historic.csv"
        val displayNYPD=spark.read.format("csv").option("inferSchema","true").load(complaintDataPath)
        val renamedData = displayNYPD.withColumnRenamed("CMPLNT_NUM","COMPLAINT-NUM").withColumnRenamed("CMPLNT_FR_DT","COMPLAINT DATE").withColumnRenamed("CMPLNT_FR_TM","COMPLAINT TIME").withColumnRenamed("CMPLNT_TO_DT","COMPLAINT END DATE").withColumnRenamed("CMPLNT_TO_TM","COMPLAINT END TIME").withColumnRenamed("ADDR_PCT_CD","NYPD PRETINCT").withColumnRenamed("RPT_DT","REPORT DATE").withColumnRenamed("KY_CD","DIGIT OFFENSE CODE").withColumnRenamed("OFNS_DESC","OFFENSE DESC").withColumnRenamed("PD_CD","INTERNAL CODE").withColumnRenamed("PD_DESC","INTERNAL CODE DESC").withColumnRenamed("CRM_ATPT_CPTD_CD","CRIME COMPLETED/NOT").withColumnRenamed("LAW_CAT_CD","LEVEL OF OFFENSE").withColumnRenamed("BORO_NM","BOROUGH NAME").withColumnRenamed("LOC_OF_OCCUR_DESC","LOCATION OF OCCURENCE").withColumnRenamed("PREM_TYP_DESC","PREMISE DESCRIPTION").withColumnRenamed("JURIS_DESC","JURISDICTION CODE").withColumnRenamed("HADEVELOPT","NAME OF HOUSING ").withColumnRenamed("HOUSING_PSA","HOUSING_CODE")
        val selectedcolumns=renamedData.select("COMPLAINT-NUM","COMPLAINT DATE","NYPD PRETINCT","REPORT DATE","DIGIT OFFENSE CODE","OFFENSE DESC","LEVEL OF OFFENSE","BOROUGH NAME","LOCATION OF OCCURENCE","JURISDICTION CODE","NAME OF HOUSING ","HOUSING_CODE","SUSP_AGE_GROUP","SUSP_RACE","SUSP_SEX","TRANSIT_DISTRICT","PATROL_BORO","STATION_NAME","VIC_AGE_GROUP","VIC_RACE","VIC_SEX")
        val filteredData= selectedcolumns.na.drop(Seq("COMPLAINT-NUM"))
        filteredData.write.format("csv").option("header","true").load("bdadProject/cleaneddataset/NYPDComplaint/NYPD_cleaned.csv")

    }
}

