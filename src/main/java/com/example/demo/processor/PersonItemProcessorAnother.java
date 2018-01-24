package com.example.demo.processor;


import com.example.demo.model.Person;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PersonItemProcessorAnother implements ItemProcessor<Person, Person>{

	@Override
	public Person process(Person person) throws Exception {
		String prefix = "Mr. ";
		if (person.getFirstName().equalsIgnoreCase("Durga")) {
			prefix = "Miss. ";
		}
		final String firstName = prefix +person.getFirstName();
		final String lastName = person.getLastName().toLowerCase();
		final Person transformedPerson = new Person(firstName, lastName);
		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		return transformedPerson;
	}
}
