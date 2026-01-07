package com.yazan.bank.controller;

import com.yazan.bank.model.Rooms;
import com.yazan.bank.service.RoomService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/rooms")
public class RoomController {
    public final RoomService roomService;

    public RoomController(RoomService roomService) {
        this.roomService = roomService;
    }

    @PostMapping
    public Rooms addRoom (@RequestParam String roomName){
        return roomService.addRoom(roomName);
    }

    @GetMapping
    public List<Rooms> getAllRooms(){
        return roomService.getAllRooms();
    }
}
