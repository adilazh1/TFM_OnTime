package com.fly.ontime.model;

import com.fly.ontime.util.AppParams;

public class Rating {

	
	private Integer ontime;
	private Integer late15;
	private Integer late30;
	private Integer late45;
	private Double delayMean;
	private Double delayStandardDeviation;
	private Double delayMin;
	private Double delayMax;
	private Double allOntimeCumulative;
	private Double allOntimeStars;
	private Double allDelayCumulative;
	private Double allDelayStars;
	
	
	
	public Rating(Integer ontime, Integer late15, Integer late30, Integer late45, Double delayMean,
			Double delayStandardDeviation, Double delayMin, Double delayMax, Double allOntimeCumulative,
			Double allOntimeStars, Double allDelayCumulative, Double allDelayStars) {
		super();
		this.ontime = ontime;
		this.late15 = late15;
		this.late30 = late30;
		this.late45 = late45;
		this.delayMean = delayMean;
		this.delayStandardDeviation = delayStandardDeviation;
		this.delayMin = delayMin;
		this.delayMax = delayMax;
		this.allOntimeCumulative = allOntimeCumulative;
		this.allOntimeStars = allOntimeStars;
		this.allDelayCumulative = allDelayCumulative;
		this.allDelayStars = allDelayStars;
	}

	public Rating() {
	}

	public static String getCSVHeaderV1() {
		return 
		"ontime_v1" + AppParams.get_csvSeparator() +
		"late15_v1" + AppParams.get_csvSeparator() +
		"late30_v1" + AppParams.get_csvSeparator() +
		"late45_v1" + AppParams.get_csvSeparator() +
		"delayMean_v1" + AppParams.get_csvSeparator() +
		"delayStandardDeviation_v1" + AppParams.get_csvSeparator() +
		"delayMin_v1" + AppParams.get_csvSeparator() +
		"delayMax_v1" + AppParams.get_csvSeparator() +
		"allOntimeCumulative_v1" + AppParams.get_csvSeparator() +
		"allOntimeStars_v1" + AppParams.get_csvSeparator() +
		"allDelayCumulative_v1" + AppParams.get_csvSeparator() +
		"allDelayStars_v1";
	}
	
	public static String getCSVHeaderV2() {
		return 
		"ontime_v2" + AppParams.get_csvSeparator() +
		"late15_v2" + AppParams.get_csvSeparator() +
		"late30_v2" + AppParams.get_csvSeparator() +
		"late45_v2" + AppParams.get_csvSeparator() +
		"delayMean_v2" + AppParams.get_csvSeparator() +
		"delayStandardDeviation_v2" + AppParams.get_csvSeparator() +
		"delayMin_v2" + AppParams.get_csvSeparator() +
		"delayMax_v2" + AppParams.get_csvSeparator() +
		"allOntimeCumulative_v2" + AppParams.get_csvSeparator() +
		"allOntimeStars_v2" + AppParams.get_csvSeparator() +
		"allDelayCumulative_v2" + AppParams.get_csvSeparator() +
		"allDelayStars_v2";
	}
	
	public String toCSVEntry() {
		String res =  
				this.ontime + AppParams.get_csvSeparator() +
				this.late15 + AppParams.get_csvSeparator() +
				this.late30 + AppParams.get_csvSeparator() +
				this.late45 + AppParams.get_csvSeparator() +
				this.delayMean + AppParams.get_csvSeparator() +
				this.delayStandardDeviation + AppParams.get_csvSeparator() +
				this.delayMin + AppParams.get_csvSeparator() +
				this.delayMax + AppParams.get_csvSeparator() +
				this.allOntimeCumulative + AppParams.get_csvSeparator() +
				this.allOntimeStars + AppParams.get_csvSeparator() +
				this.allDelayCumulative + AppParams.get_csvSeparator() +
				this.allDelayStars;
		return res;
	}
	
	public Integer getOntime() {
		return ontime;
	}

	public void setOntime(Integer ontime) {
		this.ontime = ontime;
	}

	public Integer getLate15() {
		return late15;
	}

	public void setLate15(Integer late15) {
		this.late15 = late15;
	}

	public Integer getLate30() {
		return late30;
	}

	public void setLate30(Integer late30) {
		this.late30 = late30;
	}

	public Integer getLate45() {
		return late45;
	}

	public void setLate45(Integer late45) {
		this.late45 = late45;
	}

	public Double getDelayMean() {
		return delayMean;
	}

	public void setDelayMean(Double delayMean) {
		this.delayMean = delayMean;
	}

	public Double getDelayStandardDeviation() {
		return delayStandardDeviation;
	}

	public void setDelayStandardDeviation(Double delayStandardDeviation) {
		this.delayStandardDeviation = delayStandardDeviation;
	}

	public Double getDelayMin() {
		return delayMin;
	}

	public void setDelayMin(Double delayMin) {
		this.delayMin = delayMin;
	}

	public Double getDelayMax() {
		return delayMax;
	}

	public void setDelayMax(Double delayMax) {
		this.delayMax = delayMax;
	}

	public Double getAllOntimeCumulative() {
		return allOntimeCumulative;
	}

	public void setAllOntimeCumulative(Double allOntimeCumulative) {
		this.allOntimeCumulative = allOntimeCumulative;
	}

	public Double getAllOntimeStars() {
		return allOntimeStars;
	}

	public void setAllOntimeStars(Double allOntimeStars) {
		this.allOntimeStars = allOntimeStars;
	}

	public Double getAllDelayCumulative() {
		return allDelayCumulative;
	}

	public void setAllDelayCumulative(Double allDelayCumulative) {
		this.allDelayCumulative = allDelayCumulative;
	}

	public Double getAllDelayStars() {
		return allDelayStars;
	}

	public void setAllDelayStars(Double allDelayStars) {
		this.allDelayStars = allDelayStars;
	}

	@Override
	public String toString() {
		return "Rating [ontime=" + ontime + ", late15=" + late15 + ", late30=" + late30 + ", late45=" + late45
				+ ", delayMean=" + delayMean + ", delayStandardDeviation=" + delayStandardDeviation + ", delayMin="
				+ delayMin + ", delayMax=" + delayMax + ", allOntimeCumulative=" + allOntimeCumulative
				+ ", allOntimeStars=" + allOntimeStars + ", allDelayCumulative=" + allDelayCumulative
				+ ", allDelayStars=" + allDelayStars + "]";
	}

	
	
}
