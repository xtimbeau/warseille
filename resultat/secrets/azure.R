azure_url ="https://pourpin.blob.core.windows.net/marseille"
azure_jeton = "sp=racwdlmeop&st=2023-12-09T14:36:32Z&se=2026-12-09T22:36:32Z&spr=https&sv=2022-11-02&sr=c&sig=dHQAp6L7CPBComiDMYrMohwf3q1Up6wADCnYZXCatgc%3D"

board <- pins::board_azure(
  AzureStor::storage_container(
    azure_url,
    sas = azure_jeton))

bd_hash <- function(obj) pins::pin_meta(board, obj)$pin_hash
bd_read <- function(obj) pins::pin_read(board, obj)

bd_write <- function(obj, name=NULL, title=NULL, description=NULL, metadata = NULL, tags=NULL, versioned=NULL) {
  if(is.null(name))
    name <- rlang::as_name(rlang::enquo(obj))
  pins::pin_write(board = board,
                  x = obj,
                  name = name, 
                  title = title,
                  description = description,
                  metadata = metadata,
                  tags = tags,
                  versioned = versioned,
                  type = "qs")
}