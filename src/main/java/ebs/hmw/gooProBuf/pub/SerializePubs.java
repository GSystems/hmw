package ebs.hmw.gooProBuf.pub;

import ebs.hmw.gooProBuf.pub.ProtoPub.ProtoPublication;
import ebs.hmw.model.Publication;

public class SerializePubs {

	public static ProtoPublication mapFieldsForSave(Publication publication) {

		ProtoPublication.Builder protoPublicationBuilder = ProtoPublication.newBuilder();

		protoPublicationBuilder.setCompany(publication.getCompany());
		protoPublicationBuilder.setDrop(publication.getDrop());
		protoPublicationBuilder.setId(publication.getPublicationId());
		protoPublicationBuilder.setValue(publication.getValue());
		protoPublicationBuilder.setVariation(publication.getVariation());

		// TODO map the date

		return protoPublicationBuilder.build();
	}
}
