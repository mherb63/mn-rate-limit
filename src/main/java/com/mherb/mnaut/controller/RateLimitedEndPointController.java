package com.mherb.mnaut.controller;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.LocalTime;

@Controller("/time")
@Slf4j
public class RateLimitedEndPointController {

    private static final int QUOTA_PER_MINUTE = 10;
    private final StatefulRedisConnection<String, String> redisConnection;

    public RateLimitedEndPointController(StatefulRedisConnection<String, String> redisConnection) {
        this.redisConnection = redisConnection;
    }

    @Get
    @ExecuteOn(TaskExecutors.IO)
    public String time() {
        return getTime("EXAMPLE:TIME", LocalTime.now());
    }

    @Get("/utc")
    @ExecuteOn(TaskExecutors.IO)
    public String timeUtc(){
        return getTime("EXAMPLE::UTC", LocalTime.now(Clock.systemUTC()));
    }

    private String getTime(String key, LocalTime now) {
        final String value = redisConnection.sync().get(key);
        int currentQuata = value == null ? 0 : Integer.parseInt(value);
        if (currentQuata >= QUOTA_PER_MINUTE) {
            final String err = String.format("Rate limit reached %s %s/%sw", key, currentQuata, QUOTA_PER_MINUTE);
            log.info(err);
            return err;
        }
        log.info("Current Quota {} {}/{}", key, currentQuata, QUOTA_PER_MINUTE);
        increaseCurrentQuota(key);

        return now.toString();
    }

    private void increaseCurrentQuota(String key) {
        final RedisCommands<String, String> commands = redisConnection.sync();
        commands.multi();
        commands.incrby(key, 1);
        long remainingSeconds = 60 - LocalTime.now().getSecond();
        commands.expire(key, remainingSeconds);
        commands.exec();
    }
}
