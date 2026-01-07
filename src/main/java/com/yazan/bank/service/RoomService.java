package com.yazan.bank.service;

import com.yazan.bank.model.Rooms;
import com.yazan.bank.repository.RoomRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;

@Service
public class RoomService {

    private final RoomRepository roomRepository;

    public RoomService(RoomRepository roomRepository) {
        this.roomRepository = roomRepository;
    }

    public Optional<Rooms> getRoomByName(String name) {
        return roomRepository.findByName(name);
    }

    public List<Rooms> getAllRooms() {
        return roomRepository.findAll();
    }

    public Rooms addRoom(String roomName) {
        Optional<Rooms> room = roomRepository.findByName(roomName);
        if (room.isPresent()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Room already exists");
        }

        return roomRepository.save(new Rooms(roomName));
    }
}