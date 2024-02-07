//package ec.edu.utpl.presencial.computacion.pfr.pintegra

// Importaciones de las librerias correspondientes
import com.github.tototoshi.csv.*
import java.io.File
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.effect.unsafe.implicits.global

// Delimitador de ";" para los campos de los archivos CSV
implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter {

  @main
  def exportFunc() = {
    // Archivo dsAlienacionesXTorneo
    val rutaAlTor = "C:\\Users\\Usuario\\Downloads\\ArchivoPIntegrador\\dsAlineacionesXTorneo.csv"
    val readerAlTor = CSVReader.open(new File(rutaAlTor))
    val alineacionesTorneo: List[Map[String, String]] =
      readerAlTor.allWithHeaders()
    readerAlTor.close()

    // Archivo dsPartidosYGoles
    val rutaParGol = "C:\\Users\\Usuario\\Downloads\\ArchivoPIntegrador\\dsPartidosYGoles.csv"
    val readerParGol = CSVReader.open(new File(rutaParGol))
    val partidosGoles: List[Map[String, String]] =
      readerParGol.allWithHeaders()
    readerParGol.close()


    generarAwayTeams(partidosGoles)
    generarGoals(partidosGoles)
    generarHomeTeams(partidosGoles)
    GenerarMatches(partidosGoles)


  }

  // Metodo para el reemplazo del valor NA por 0 
  def limpiar(text: String): Double =
    if (text.equals("NA")) {
      0
    } else {
      text.toDouble
    }

  // Metodo para la insercíón del escape de las comillas simples
  def quoteEscape(text: String) =
    text.replaceAll("'", "\\\\'")

  // Funciones para el 1er método de inserción de datos
  def generarAwayTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO away_teams(matches_away_team_id, away_team_name, away_mens_team, away_womens_team, away_region_name) VALUES('%s', '%s', %d, %d, '%s');"
    val awayTeamsTuple =
      data.distinctBy(_("matches_away_team_id"))
        .map(
          row => (row("matches_away_team_id"),
            row("away_team_name"),
            limpiar(row("away_mens_team")).toInt,
            limpiar(row("away_womens_team")).toInt,
            row("away_region_name"))
        )
        .sorted
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5))
    awayTeamsTuple.foreach(println)

  }


  def generarGoals(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO goals(goals_goal_id, goals_team_id, goals_player_id, goals_player_team_id, goals_minute_label, goals_minute_regulation, goals_minute_stoppage, goals_match_period, goals_own_goal, goals_penalty) VALUES('%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %d, %d);"
    val goalsTuple = data
      .map(
        row => (row("goals_goal_id"),
          row("goals_team_id"),
          row("goals_player_id"),
          row("goals_player_team_id"),
          quoteEscape(row("goals_minute_label")),
          limpiar(row("goals_minute_regulation")).toInt,
          limpiar(row("goals_minute_stoppage")).toInt,
          row("goals_match_period"),
          limpiar(row("goals_own_goal")).toInt,
          limpiar(row("goals_penalty")).toInt)
      )
      .filterNot(_._1.equals("NA"))
      .sortBy(_._1)
      .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5, name._6, name._7, name._8, name._9, name._10))

    // Se imprime en pantalla en dos partes las instrucciones
    goalsTuple.take(goalsTuple.length / 2).foreach(println)
    goalsTuple.drop(goalsTuple.length / 2).foreach(println)
  }

  def generarHomeTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO home_teams(matches_home_team_id, home_team_name, home_mens_team, home_womens_team, home_region_name) VALUES('%s', '%s', %d, %d, '%s');"
    val homeTeamsTuple =
      data.distinctBy(_("matches_home_team_id"))
        .map(
          row => (row("matches_home_team_id"),
            row("home_team_name"),
            limpiar(row("home_mens_team")).toInt,
            limpiar(row("home_womens_team")).toInt,
            row("home_region_name"))
        )
        .sorted
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5))

    homeTeamsTuple.foreach(println)
  }

  def GenerarMatches(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO matches(matches_match_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_stage_name, matches_match_time, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result, tournaments_tournament_name, tournaments_year) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, '%s', '%s', '%s');"
    val matchesTuple =
      data.distinctBy(_("matches_match_id"))
        .map(
          row => (row("matches_match_id"),
            row("matches_away_team_id"),
            row("matches_home_team_id"),
            row("matches_stadium_id"),
            row("matches_match_date"),
            row("matches_stage_name"),
            row("matches_match_time"),
            limpiar(row("matches_home_team_score")).toInt,
            limpiar(row("matches_away_team_score")).toInt,
            limpiar(row("matches_extra_time")).toInt,
            limpiar(row("matches_penalty_shootout")).toInt,
            limpiar(row("matches_home_team_score_penalties")).toInt,
            limpiar(row("matches_away_team_score_penalties")).toInt,
            row("matches_result"),
            quoteEscape(row("tournaments_tournament_name")),
            row("tournaments_year")))
        .sortBy(_._1)
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5, name._6, name._7, name._8, name._9,
          name._10, name._11, name._12, name._13, name._14, name._15, name._16))

    matchesTuple.foreach(println)

  }

}



