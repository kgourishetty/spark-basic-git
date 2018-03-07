package com.sparktesting.model;

import java.io.Serializable;

public class HealthDataModel implements Serializable,Comparable<HealthDataModel> {

	private String educationLevel;
	private long age;
	private String ageRange;
	private String employmentStatus;
	private String gender;
	private long children;
	private long weeklyEarnings;
	private long year;
	private long weeklyHoursWorked;
	private long sleeping;
	private long grooming;
	private long housework;
	private long foodDrinkPrep;
	private long caringforChildren;
	private long playingwithChildren;
	private long jobSearching;
	private long shopping;
	private long eatingandDrinking;
	private long socializingRelaxing;
	private long television;
	private long golfing;
	private long running;
	private long volunteering;
	
	public String getEducationLevel() {
		return educationLevel;
	}
	public void setEducationLevel(String educationLevel) {
		this.educationLevel = educationLevel;
	}
	public long getAge() {
		return age;
	}
	public void setAge(long age) {
		this.age = age;
	}
	public String getAgeRange() {
		return ageRange;
	}
	public void setAgeRange(String ageRange) {
		this.ageRange = ageRange;
	}
	public String getEmploymentStatus() {
		return employmentStatus;
	}
	public void setEmploymentStatus(String employmentStatus) {
		this.employmentStatus = employmentStatus;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public long getChildren() {
		return children;
	}
	public void setChildren(long children) {
		this.children = children;
	}
	public long getWeeklyEarnings() {
		return weeklyEarnings;
	}
	public void setWeeklyEarnings(long weeklyEarnings) {
		this.weeklyEarnings = weeklyEarnings;
	}
	public long getYear() {
		return year;
	}
	public void setYear(long year) {
		this.year = year;
	}
	public long getWeeklyHoursWorked() {
		return weeklyHoursWorked;
	}
	public void setWeeklyHoursWorked(long weeklyHoursWorked) {
		this.weeklyHoursWorked = weeklyHoursWorked;
	}
	public long getSleeping() {
		return sleeping;
	}
	public void setSleeping(long sleeping) {
		this.sleeping = sleeping;
	}
	public long getGrooming() {
		return grooming;
	}
	public void setGrooming(long grooming) {
		this.grooming = grooming;
	}
	public long getHousework() {
		return housework;
	}
	public void setHousework(long housework) {
		this.housework = housework;
	}
	public long getFoodDrinkPrep() {
		return foodDrinkPrep;
	}
	public void setFoodDrinkPrep(long foodDrinkPrep) {
		this.foodDrinkPrep = foodDrinkPrep;
	}
	public long getCaringforChildren() {
		return caringforChildren;
	}
	public void setCaringforChildren(long caringforChildren) {
		this.caringforChildren = caringforChildren;
	}
	public long getPlayingwithChildren() {
		return playingwithChildren;
	}
	public void setPlayingwithChildren(long playingwithChildren) {
		this.playingwithChildren = playingwithChildren;
	}
	public long getJobSearching() {
		return jobSearching;
	}
	public void setJobSearching(long jobSearching) {
		this.jobSearching = jobSearching;
	}
	public long getShopping() {
		return shopping;
	}
	public void setShopping(long shopping) {
		this.shopping = shopping;
	}
	public long getEatingandDrinking() {
		return eatingandDrinking;
	}
	public void setEatingandDrinking(long eatingandDrinking) {
		this.eatingandDrinking = eatingandDrinking;
	}
	public long getSocializingRelaxing() {
		return socializingRelaxing;
	}
	public void setSocializingRelaxing(long socializingRelaxing) {
		this.socializingRelaxing = socializingRelaxing;
	}
	public long getTelevision() {
		return television;
	}
	public void setTelevision(long television) {
		this.television = television;
	}
	public long getGolfing() {
		return golfing;
	}
	public void setGolfing(long golfing) {
		this.golfing = golfing;
	}
	public long getRunning() {
		return running;
	}
	public void setRunning(long running) {
		this.running = running;
	}
	public long getVolunteering() {
		return volunteering;
	}
	public void setVolunteering(long volunteering) {
		this.volunteering = volunteering;
	}
	@Override
	public int compareTo(HealthDataModel o) {
		// TODO Auto-generated method stub
		return Long.compare(this.getGolfing(), o.getGolfing());
	}
	
	

}
