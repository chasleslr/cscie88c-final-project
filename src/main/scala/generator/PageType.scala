package generator

object PageType extends Enumeration {
  type PageType = String
  val SEARCH = "search"
  val CATEGORY = "category"
  val SIMILAR = "similar"
}
