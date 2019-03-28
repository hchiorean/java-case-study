package com.trivago.mp.casestudy.data;

import com.trivago.mp.casestudy.Advertiser;
import com.trivago.mp.casestudy.Hotel;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A {@link DataProvider} implementation which loads some test data from CSV files using a predefined structure convention.
 *
 * @author Horia Chiorean
 */
public class CSVDataProvider implements DataProvider {

	static private class HotelAndCity {
		private final Integer cityId;
		private final String cityName;
		private final Hotel hotel;

		private HotelAndCity(Integer cityId, String cityName, Hotel hotel) {
			this.cityId = cityId;
			this.hotel = hotel;
			this.cityName = cityName;
		}
	}

	static private class AdvertisedHotel {
		private final Integer advertiserId;
		private final Integer hotelId;

		private AdvertisedHotel(Integer advertiserId, Integer hotelId) {
			this.advertiserId = advertiserId;
			this.hotelId = hotelId;
		}

		Integer getAdvertiserId() {
			return advertiserId;
		}

		Integer getHotelId() {
			return hotelId;
		}
	}

	static private class CSVParser {
		static private void parse(InputStream is, Consumer<String[]> lineConsumer) {
			parse(is, lineConsumer, true, ",", StandardCharsets.UTF_8);
		}

		static private void parse(InputStream is, Consumer<String[]> lineConsumer, boolean skipHeader, String fieldSeparator, Charset charset) {
			try(BufferedReader br = new BufferedReader(new InputStreamReader(is, charset))) {
				br.lines()
				.skip(skipHeader ? 1 : 0)
				.map(line -> line.split(fieldSeparator))
				.forEach(lineConsumer);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private CSVParser() {
			// this is a minor utility and should not be exposed
		}
	}

	static private FileInputStream getFileInputStream(String filePath) {
		filePath = Objects.requireNonNull(filePath, "filePath");
		File file = new File(filePath);
		try {
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("File " + filePath + " is not an existing readable file or is a directory");
		}
	}

	static private int parseInt(String idStr) {
		try {
			return Integer.valueOf(idStr);
		} catch (NumberFormatException | NullPointerException e) {
			throw new IllegalArgumentException("Invalid integer: " + idStr);
		}
	}

	static private final AtomicReference<CSVDataProvider> defaultInstance = new AtomicReference<>();

	static public DataProvider getDefaultInstance() {
		CSVDataProvider provider = defaultInstance.get();
		if(provider != null)
			return provider;
		provider = new CSVDataProvider();
		if(defaultInstance.compareAndSet(null, provider))
			provider.loadAllData();

		return defaultInstance.get();
	}

	private final String advertisersFile;
	private final String hotelsFile;
	private final String hotelsAdvertisersFile;
	private final String citiesFile;

	private final Map<Integer, HotelAndCity> hotelsAndCitiesByHotelId = new HashMap<>();
	private final Map<Integer, Advertiser> advertisersById = new HashMap<>();
	private final Map<String, List<AdvertisedHotel>> advertisedHotelsByCity = new HashMap<>();

	private CSVDataProvider() {
		this("data/advertisers.csv", "data/hotels.csv", "data/hotel_advertiser.csv", "data/cities.csv");
	}

	CSVDataProvider(String advertisersFile, String hotelsFile, String hotelsAdvertisersFile, String citiesFile) {
		this.advertisersFile = advertisersFile;
		this.hotelsFile = hotelsFile;
		this.hotelsAdvertisersFile = hotelsAdvertisersFile;
		this.citiesFile = citiesFile;
	}

	void loadAllData() {
		Map<Integer, String> citiesById = loadCities();
		loadHotels(citiesById);

		// The following can be run independently (via the fork join pool executor)
		CompletableFuture<Void> advertisersLoader = CompletableFuture.runAsync(this::loadAdvertisers);
		CompletableFuture<Void> hotelAndAdvertisersLoader = CompletableFuture.runAsync(this::loadHotelsAndAdvertisers);

		CompletableFuture.allOf(advertisersLoader, hotelAndAdvertisersLoader)
		.join();
	}

	@Override
	public Advertiser getAdvertiserById(Integer advertiserId) {
		return advertisersById.get(advertiserId);
	}

	@Override
	public Hotel getHotelById(Integer hotelId) {
		HotelAndCity hotelAndCity = hotelsAndCitiesByHotelId.get(hotelId);
		return hotelAndCity != null ? hotelAndCity.hotel : null;
	}

	@Override
	public Map<Integer, List<Integer>> getHotelIdsByAdvertiser(String cityName) {
		if(cityName == null || cityName.trim().length() == 0)
			throw new IllegalArgumentException("The name of the city is required");

		List<AdvertisedHotel> advertisedHotels = advertisedHotelsByCity.get(cityName);
		if(advertisedHotels == null)
			return Collections.emptyMap();

		return advertisedHotels
		.parallelStream()
		.collect(Collectors.groupingBy(AdvertisedHotel::getAdvertiserId, Collectors.mapping(AdvertisedHotel::getHotelId, Collectors.toList())));
	}

	private void loadHotelsAndAdvertisers() {
		CSVParser.parse(getFileInputStream(hotelsAdvertisersFile), parts -> {
			if(parts.length != 2)
				throw new IllegalArgumentException("Invalid number advertiser-hotel parts: " + Arrays.toString(parts) + "; expected 2");

			Integer hotelId = Integer.valueOf(parseInt(parts[1]));
			Integer advertiserId = Integer.valueOf(parseInt(parts[0]));

			HotelAndCity hotelAndCity = hotelsAndCitiesByHotelId.get(hotelId);
			advertisedHotelsByCity.computeIfAbsent(hotelAndCity.cityName, cityName -> new ArrayList<>()).add(new AdvertisedHotel(advertiserId, hotelId));
		});
	}

	private void loadAdvertisers() {
		CSVParser.parse(getFileInputStream(advertisersFile), parts -> {
			if(parts.length != 2)
				throw new IllegalArgumentException("Invalid number advertiser parts: " + Arrays.toString(parts) + "; expected 2");

			int advertiserId = parseInt(parts[0]);
			advertisersById.put(Integer.valueOf(advertiserId), new Advertiser(advertiserId, parts[1]));
		});
	}

	private void loadHotels(Map<Integer, String> citiesById) {
		CSVParser.parse(getFileInputStream(hotelsFile), parts -> {
			if(parts.length != 7)
				throw new IllegalArgumentException("Invalid hotel parts: " + Arrays.toString(parts) +  "; expected 7");

			Integer hotelId = Integer.valueOf(parseInt(parts[0]));
			Integer cityId = Integer.valueOf(parseInt(parts[1]));
			String name = parts[4];
			int rating = parseInt(parts[5]);
			int stars = parseInt(parts[6]);
			Hotel hotel = new Hotel(hotelId, name, rating, stars);
			hotelsAndCitiesByHotelId.put(hotelId, new HotelAndCity(cityId, citiesById.get(cityId), hotel));
		});
	}

	private Map<Integer, String> loadCities() {
		Map<Integer, String> citiesById = new HashMap<>();
		CSVParser.parse(getFileInputStream(citiesFile), parts -> {
			Integer id = Integer.valueOf(parts[0]);
			String name = parts[1];
			citiesById.put(id, name);
		});
		return citiesById;
	}
}