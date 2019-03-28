package com.trivago.mp.casestudy.data;

import com.trivago.mp.casestudy.Advertiser;
import com.trivago.mp.casestudy.Hotel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Horia Chiorean
 */
public class CSVDataProviderTest {

	@Test
	public void testGetHotelById() {
		DataProvider dataProvider = CSVDataProvider.getDefaultInstance();
		assertHotel(dataProvider.getHotelById(Integer.valueOf(0)), "hotel_basic_with_a_view_Berlin", 67, 4);
		assertHotel(dataProvider.getHotelById(Integer.valueOf(553)), "hotel_superior_resort_Vienna", 89, 2);
	}

	private void assertHotel(Hotel hotel, String expectedName, int expectedRating, int expectedStars) {
		assertEquals(expectedName, hotel.getName());
		assertEquals(expectedRating, hotel.getRating());
		assertEquals(expectedStars, hotel.getStars());
	}

	@Test
	public void testGeAdvertiserById() {
		DataProvider dataProvider = CSVDataProvider.getDefaultInstance();
		asserAdvertiser(dataProvider.getAdvertiserById(Integer.valueOf(0)), "travel_advisor_foryou");
		asserAdvertiser(dataProvider.getAdvertiserById(Integer.valueOf(99)), "relax_go_worldwide");
	}

	private void asserAdvertiser(Advertiser advertiser, String expectedName) {
		assertEquals(expectedName, advertiser.getName());
	}

	@Test
	public void testGetHotelsAndAdvertisersByCity() {
		// TODO
	}
}