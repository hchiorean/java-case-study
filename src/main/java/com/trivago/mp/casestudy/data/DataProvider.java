package com.trivago.mp.casestudy.data;

import com.trivago.mp.casestudy.Advertiser;
import com.trivago.mp.casestudy.Hotel;

import java.util.List;
import java.util.Map;

/**
 * A provider of data for the scope of the exercise.
 *
 * @author Horia Chiorean
 */
public interface DataProvider {

	Advertiser getAdvertiserById(Integer advertiserID);

	Hotel getHotelById(Integer hotelID);

	/**
	 * @param cityName the name of a city, expected to be not null.
	 * @return a {@link Map} of [advertiserId, List(hotelId)] pairs for the given city, never {@code null} but possibly empty
	 *
	 * @throws IllegalArgumentException if the city is null or empty
	 *
	 * //TODO: Ideally we'd use Advertiser instance directly as keys, but assume for this exercise we have no control over them so we can't get a hashCode/equals
	 */
	Map<Integer, List<Integer>> getHotelIdsByAdvertiser(String cityName);
}