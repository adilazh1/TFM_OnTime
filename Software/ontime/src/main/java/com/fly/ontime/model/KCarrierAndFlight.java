package com.fly.ontime.model;

public class KCarrierAndFlight {

	
	private String carrierFsCode;
	private String flightNumber;
	
	
//	public KCarrierAndFlight() {
//	}


	public KCarrierAndFlight(String carrierFsCode, String flightNumber) {
		super();
		this.carrierFsCode = carrierFsCode;
		this.flightNumber = flightNumber;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((carrierFsCode == null) ? 0 : carrierFsCode.hashCode());
		result = prime * result + ((flightNumber == null) ? 0 : flightNumber.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KCarrierAndFlight other = (KCarrierAndFlight) obj;
		if (carrierFsCode == null) {
			if (other.carrierFsCode != null)
				return false;
		} else if (!carrierFsCode.equals(other.carrierFsCode))
			return false;
		if (flightNumber == null) {
			if (other.flightNumber != null)
				return false;
		} else if (!flightNumber.equals(other.flightNumber))
			return false;
		return true;
	}



	public String toString2() {
		return "KCarrierAndFlight [carrierFsCode=" + carrierFsCode + ", flightNumber=" + flightNumber + "]";
	}
	
	@Override	
	public String toString() {
		return "(" + carrierFsCode + "-" + flightNumber + ")";
	}
	




	
	
}
