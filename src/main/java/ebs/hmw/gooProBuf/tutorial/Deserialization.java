package ebs.hmw.gooProBuf.tutorial;

import ebs.hmw.gooProBuf.tutorial.AddressBookProtos.AddressBook;
import ebs.hmw.gooProBuf.tutorial.AddressBookProtos.Person;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Deserialization {

	// Iterates though all people in the AddressBook and prints info about them.
	static void Print(AddressBook addressBook) {
		for (Person person : addressBook.getPeopleList()) {
			System.out.println("Person ID: " + person.getId());
			System.out.println("  Name: " + person.getName());
			if (person.getEmail() != "") {
				System.out.println("  E-mail address: " + person.getEmail());
			}

			for (Person.PhoneNumber phoneNumber : person.getPhonesList()) {
				switch (phoneNumber.getType()) {
				case MOBILE:
					System.out.print("  Mobile phone #: ");
					break;
				case HOME:
					System.out.print("  Home phone #: ");
					break;
				case WORK:
					System.out.print("  Work phone #: ");
					break;
				}
				System.out.println(phoneNumber.getNumber());
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String path = "C:\\projects\\git\\hmw\\addressbook";
		AddressBook addressBook = AddressBook.parseFrom(new FileInputStream(path));

		Print(addressBook);
	}
}
