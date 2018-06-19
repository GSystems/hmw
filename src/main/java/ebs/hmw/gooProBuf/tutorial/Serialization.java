package ebs.hmw.gooProBuf.tutorial;

import ebs.hmw.gooProBuf.tutorial.AddressBookProtos.*;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Serialization {

	static Person PromptForAddress(BufferedReader stdin, PrintStream stdout) throws IOException {

		Person.Builder person = Person.newBuilder();

		stdout.print("Enter person ID: ");
		person.setId(Integer.valueOf(stdin.readLine()));

		stdout.print("Enter name: ");
		person.setName(stdin.readLine());

		stdout.print("Enter email address (blank for none): ");
		String email = stdin.readLine();
		if (email.length() > 0) {
			person.setEmail(email);
		}

		while (true) {
			stdout.print("Enter a phone number (or leave blank to finish): ");
			String number = stdin.readLine();
			if (number.length() == 0) {
				break;
			}

			Person.PhoneNumber.Builder phoneNumber = Person.PhoneNumber.newBuilder().setNumber(number);

			stdout.print("Is this a mobile, home, or work phone? ");
			String type = stdin.readLine();
			if (type.equals("mobile")) {
				phoneNumber.setType(Person.PhoneType.MOBILE);
			} else if (type.equals("home")) {
				phoneNumber.setType(Person.PhoneType.HOME);
			} else if (type.equals("work")) {
				phoneNumber.setType(AddressBookProtos.Person.PhoneType.WORK);
			} else {
				stdout.println("Unknown phone type.  Using default.");
			}

			person.addPhones(phoneNumber);
		}

		return person.build();
	}

	public static void main(String[] args) throws IOException {
		AddressBook.Builder addressBook = AddressBook.newBuilder();
		// Read the existing address book.
//		String path = "C:\\projects\\git\\hmw\\addressbook";
		File file = new File(getResourcePath() + "/addressbook");
		try {
			addressBook.mergeFrom(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			System.out.println(file + ": File not found.  Creating a new file.");
		}

		// Add an address.
		addressBook.addPeople(PromptForAddress(new BufferedReader(new InputStreamReader(System.in)), System.out));

		// Write the new address book back to disk.
		FileOutputStream output = new FileOutputStream(file);
		addressBook.build().writeTo(output);
		output.close();
	}

	private static String getResourcePath() {
		try {
			URI resourcePathFile = System.class.getResource("/src/main/resources").toURI();
			String resourcePath = Files.readAllLines(Paths.get(resourcePathFile)).get(0);
			URI rootURI = new File("").toURI();
			URI resourceURI = new File(resourcePath).toURI();
			URI relativeResourceURI = rootURI.relativize(resourceURI);
			return relativeResourceURI.getPath();
		} catch (Exception e) {
			return null;
		}
	}
}