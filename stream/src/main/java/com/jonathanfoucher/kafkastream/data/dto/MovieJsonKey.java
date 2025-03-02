package com.jonathanfoucher.kafkastream.data.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class MovieJsonKey {
    private Long id;

    @Override
    public String toString() {
        return String.format("{ id=%s }", id);
    }
}
