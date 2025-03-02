package com.jonathanfoucher.kafkaproducer.controllers;

import com.jonathanfoucher.kafkaproducer.data.dto.MovieValue;
import com.jonathanfoucher.kafkaproducer.services.MovieService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/movies")
public class MovieController {
    private final MovieService movieService;

    @PostMapping
    public void saveMovie(@RequestBody MovieValue movieValue) {
        movieService.saveMovie(movieValue);
    }

    @DeleteMapping("/{movie_id}")
    public void deleteMovie(@PathVariable("movie_id") Long movieId) {
        movieService.deleteMovie(movieId);
    }
}
