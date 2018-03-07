package opennlp.tools.doccat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Map;
import opennlp.model.AbstractModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.model.BaseModel;

public class DoccatModel
  extends BaseModel implements Serializable
{
  private static final String COMPONENT_NAME = "DocumentCategorizerME";
  private static final String DOCCAT_MODEL_ENTRY_NAME = "doccat.model";
  
  protected DoccatModel(String languageCode, AbstractModel doccatModel, Map<String, String> manifestInfoEntries)
  {
    super("DocumentCategorizerME", languageCode, manifestInfoEntries);
    
    this.artifactMap.put("doccat.model", doccatModel);
    checkArtifactMap();
  }
  
  public DoccatModel(String languageCode, AbstractModel doccatModel)
  {
    this(languageCode, doccatModel, null);
  }
  
  public DoccatModel(InputStream in)
    throws IOException, InvalidFormatException
  {
    super("DocumentCategorizerME", in);
  }
  
  public DoccatModel(File modelFile)
    throws IOException, InvalidFormatException
  {
    super("DocumentCategorizerME", modelFile);
  }
  
  public DoccatModel(URL modelURL)
    throws IOException, InvalidFormatException
  {
    super("DocumentCategorizerME", modelURL);
  }
  
  protected void validateArtifactMap()
    throws InvalidFormatException
  {
    super.validateArtifactMap();
    if (!(this.artifactMap.get("doccat.model") instanceof AbstractModel)) {
      throw new InvalidFormatException("Doccat model is incomplete!");
    }
  }
  
  public AbstractModel getChunkerModel()
  {
    return (AbstractModel)this.artifactMap.get("doccat.model");
  }
}
