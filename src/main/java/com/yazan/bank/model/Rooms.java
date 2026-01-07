package com.yazan.bank.model;

import javax.persistence.*;

@Entity
@Table(name = "Rooms")
public class Rooms {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "rooms_seq")
  @SequenceGenerator(name = "rooms_seq", sequenceName = "rooms_seq", allocationSize = 1)
  private Long id;
  private String name;

  public Rooms() {

  }

  public Rooms(String name) {
    this.name = name;
  }

  public  Long getId(){
    return id;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public void setId(Long id) {
    this.id = id;
  }
}