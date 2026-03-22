package com.jonathanfoucher.kafkastream.data.dto;

import lombok.Getter;
import lombok.Setter;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

import java.time.LocalDate;

@Getter
@Setter
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class MovieJsonValue {
    private Long id;
    private String title;
    private LocalDate releaseDate;

    @Override
    public String toString() {
        return String.format("{ id=%s, title=\"%s\", release_date=%s }", id, title, releaseDate);
    }
}
