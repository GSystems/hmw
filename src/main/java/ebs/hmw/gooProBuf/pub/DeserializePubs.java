package ebs.hmw.gooProBuf.pub;

import ebs.hmw.gooProBuf.pub.ProtoPub.ProtoPublication;
import ebs.hmw.gooProBuf.pub.ProtoPub.ProtoPublicationList;
import ebs.hmw.model.Publication;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeserializePubs {

	private static final String FILE_NAME = "publicationList";

	List<Publication> readPublications(ProtoPublicationList publicationList) {

		List<Publication> publications = new ArrayList<>();
		List<ProtoPublication> protoPublications = new ArrayList<>();

		try {
			protoPublications = ProtoPublicationList.parseFrom(new FileInputStream(FILE_NAME)).getPublicationList();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (ProtoPublication protoPublication : protoPublications) {
			publications.add(mapFields(protoPublication));
		}


		return publications;
	}

	private Publication mapFields(ProtoPublication protoPublication) {
		Publication publication = new Publication();

		publication.setCompany(protoPublication.getCompany());
		publication.setDrop(protoPublication.getDrop());
		publication.setPublicationId(protoPublication.getId());
		publication.setValue(protoPublication.getValue());
		publication.setVariation(protoPublication.getVariation());

		return publication;
	}
}
