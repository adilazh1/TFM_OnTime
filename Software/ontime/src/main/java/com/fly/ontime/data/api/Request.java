package com.fly.ontime.data.api;


import java.util.Map;

import org.apache.log4j.Logger;



@SuppressWarnings("rawtypes")
public class Request implements IRequest
{
	private static Logger _logger = Logger.getLogger(Request.class);

	protected String _url;

	protected Map _suffixParams;
	protected Map _params;
	


	public Map get_suffixParams() {
		return _suffixParams;
	}

	public Map get_params() {
		return _params;
	}

	public String get_url() {
		return _url;
	}

	@Override
	public String get() throws Exception {
		throw new Exception("Request::get() not implemented");
	}


	
}
