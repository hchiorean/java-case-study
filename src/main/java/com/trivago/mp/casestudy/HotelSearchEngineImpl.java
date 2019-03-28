package com.trivago.mp.casestudy;


import com.trivago.mp.casestudy.data.CSVDataProvider;
import com.trivago.mp.casestudy.data.DataProvider;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * TODO: Implement this class.
 * Your task will be to implement two functions, one for loading the data which is stored as .csv files in the ./data
 * folder and one for performing the actual search.
 */
public class HotelSearchEngineImpl implements HotelSearchEngine {

    @Override
    public void initialize() {
        // This will trigger the initialization
        CSVDataProvider.getDefaultInstance();
    }

    /**
     * The following doesn't handle any extra-ordinary stuff around the {@link OfferProvider} like:
     * - network issues, unexpected errors, retries etc
     * - potential caching/chunking of information
     * <p>
     * A nice provider would handle all these
     */
    @Override
    public List<HotelWithOffers> performSearch(String cityName, DateRange dateRange, OfferProvider offerProvider) {
        Objects.requireNonNull(cityName, "cityName");
        Objects.requireNonNull(dateRange, "dateRange");
        if(dateRange.getStartDate() > dateRange.getEndDate())
            throw new IllegalArgumentException("Invalid date rage: " + dateRange);
        Objects.requireNonNull(offerProvider, "offerProvider");

        DataProvider dataProvider = CSVDataProvider.getDefaultInstance();

        Map<Integer, List<Integer>> hotelIdsByAdvertiser = dataProvider.getHotelIdsByAdvertiser(cityName);

        // Request offers for each advertiser and list of hotels for that advertiser
        Map<Integer, List<Offer>> offersByHotelId = hotelIdsByAdvertiser.entrySet()
        .parallelStream()
        .flatMap(entry -> offerProvider.getOffersFromAdvertiser(dataProvider.getAdvertiserById(entry.getKey()), entry.getValue(), dateRange).entrySet().stream())
        .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

        // Then convert them into the expected output and sort them by [rating DESC, stars DESC]
        // Note this is a real superficial sort, in real life this could be a lot more complicated
       return offersByHotelId.entrySet()
       .stream()
       .map(entry -> {
           HotelWithOffers hotelWithOffers = new HotelWithOffers(dataProvider.getHotelById(entry.getKey()));
           //TODO: It would've been a lot nicer if there was a constructor with the list
           hotelWithOffers.setOffers(entry.getValue());
           return hotelWithOffers;
       })
       .sorted(
            java.util.Comparator.<HotelWithOffers>comparingInt(hotelWithOffers -> hotelWithOffers.getHotel().getRating())
            .thenComparingInt(hotelWithOffers -> hotelWithOffers.getHotel().getStars())
            .reversed()
        )
       .collect(Collectors.toList());
    }
}
