import com.github.tototoshi.csv.*
import doobie.*
import doobie.implicits.*

import java.io.File
import doobie.*
import doobie.implicits.*
import cats.{data, *}
import cats.effect.*
import cats.implicits.*
import cats.effect.unsafe.implicits.global
import scala.language.postfixOps

object Exporter {
  @main
  def exportFunc() =
    val path2DataFile = "C:\\Users\\jeana\\Desktop\\Practicum\\dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()
    reader.close()

    val path2DataFile2 = "C:\\Users\\jeana\\Desktop\\Practicum\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] =
      reader2.allWithHeaders()
    reader2.close()

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/proyecto_integrador",
      user = "root",
      password = "RocketManProtocol621@",
      logHandler = None
    )
    //Insertar directo en la abse de datos
    //Stadium
    //generateData2StadiumsDoobie(contentFile).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    //Players
    //generateData2PlayersDoobie(contentFile2).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    //Tournaments
    //generateData2TournamentsDoobie(contentFile).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    //Squads
    //generateData2SquadsDoobie(contentFile2).foreach(insert => insert.run.transact(xa).unsafeRunSync())

    //Scripts para insertar los datos en la base de datos
    //generarAwayTeams(contentFile)
    //generarGoals(contentFile)
    //generarHomeTeams(contentFile)
    //GenerarMatches(contentFile)

    //Preguntas planteadas
    //Cual es el top 10 de los maximos goleadores de la historia?
    pregunta1().transact(xa).unsafeRunSync().foreach(println)
    println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    //Cual es el top 10 de los estadios en los que mas partidos se han jugado?
    pregunta2().transact(xa).unsafeRunSync().foreach(println)
    println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    //Cuantos goles en total ha anotado cada genero?
    pregunta3().transact(xa).unsafeRunSync().foreach(println)
    println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    //Cual es el top 10 de los anios en los que se jugaron mas partidos?
    pregunta4().transact(xa).unsafeRunSync().foreach(println)


  def defaultValue(text: String): Double =
    if (text.equals("NA")) {
      0
    } else {
      text.toDouble
    }

  def defaultValue2(text: String): String =
    if (text.equals("NA")) {
      "0"
    } else if (text.equals("not available")) {
      "empty"
    } else if (text.equals("not applicable")) {
      "empty"
    } else {
      text
    }

  def generateData2StadiumsDoobie(data: List[Map[String, String]]) =
    val stadiumTuple = data
      .map(
        row => (row("matches_stadium_id"),
          row("stadiums_stadium_name"),
          row("stadiums_city_name"),
          row("stadiums_country_name"),
          defaultValue(row("stadiums_stadium_capacity")).toInt)
      )
      .distinct
      .map(ts => sql"INSERT INTO stadiums(stadium_id, stadium_name,city_name,country_name,stadium_capacity) VALUES (${ts._1},${ts._2},${ts._3},${ts._4},${ts._5})".update)
    stadiumTuple

  def generateData2PlayersDoobie(data: List[Map[String, String]]) =
    val playerTuple = data
      .map(
        row => (row("squads_player_id"),
          row("players_family_name"),
          defaultValue2(row("players_given_name")),
          defaultValue2(row("players_birth_date")),
          defaultValue(row("players_female")).toInt)
      )
      .distinct
      .map(tp => sql"INSERT INTO players(player_id, family_name,given_name,birth_date,gender) VALUES (${tp._1},${tp._2},${tp._3},${tp._4},${tp._5})".update)
    playerTuple

  def generateData2TournamentsDoobie(data: List[Map[String, String]]) =
    val tournamentTuple = data
      .map(
        row => (row("matches_tournament_id"),
          row("tournaments_tournament_name"),
          defaultValue(row("tournaments_year")).toInt,
          row("tournaments_host_country"),
          row("tournaments_winner"),
          defaultValue(row("tournaments_count_teams")).toInt)
      )
      .distinct
      .map(tt => sql"INSERT INTO tournaments(matches_tournament_id, tournaments_tournament_name,tournaments_year,tournaments_host_country,tournaments_winner,tournaments_count_teams) VALUES (${tt._1},${tt._2},${tt._3},${tt._4},${tt._5},${tt._6})".update)
    tournamentTuple

  def generateData2SquadsDoobie(data: List[Map[String, String]]) =
    val squadsTuple = data
      .map(
        row => (row("squads_team_id"),
          row("squads_tournament_id"),
          row("squads_player_id"),
          defaultValue(row("squads_shirt_number")).toInt,
          row("squads_position_name"),
          defaultValue(row("players_goal_keeper")).toInt,
          defaultValue(row("players_defender")).toInt,
          defaultValue(row("players_midfielder")).toInt,
          defaultValue(row("players_forward")).toInt)
      )
      .distinct
      .map(ts => sql"INSERT INTO squads(squads_team_id, squads_tournament_id,squads_player_id,squads_shirt_number,squads_position_name,players_goal_keeper,players_defender, players_midfielder,players_forward) VALUES (${ts._1},${ts._2},${ts._3},${ts._4},${ts._5},${ts._6},${ts._7},${ts._8},${ts._9})".update)
    squadsTuple


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
            defaultValue(row("away_mens_team")).toInt,
            defaultValue(row("away_womens_team")).toInt,
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
          defaultValue(row("goals_minute_regulation")).toInt,
          defaultValue(row("goals_minute_stoppage")).toInt,
          row("goals_match_period"),
          defaultValue(row("goals_own_goal")).toInt,
          defaultValue(row("goals_penalty")).toInt)
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
            defaultValue(row("home_mens_team")).toInt,
            defaultValue(row("home_womens_team")).toInt,
            row("home_region_name"))
        )
        .sorted
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5))
    homeTeamsTuple.foreach(println)
  }

  def GenerarMatches(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO matches(matches_tournament_id, matches_match_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_stage_name, matches_match_time, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, '%s');"
    val matchesTuple =
      data.distinctBy(_("matches_match_id"))
        .map(
          row => (row("matches_tournament_id"),
            row("matches_match_id"),
            row("matches_away_team_id"),
            row("matches_home_team_id"),
            row("matches_stadium_id"),
            row("matches_match_date"),
            row("matches_stage_name"),
            row("matches_match_time"),
            defaultValue(row("matches_home_team_score")).toInt,
            defaultValue(row("matches_away_team_score")).toInt,
            defaultValue(row("matches_extra_time")).toInt,
            defaultValue(row("matches_penalty_shootout")).toInt,
            defaultValue(row("matches_home_team_score_penalties")).toInt,
            defaultValue(row("matches_away_team_score_penalties")).toInt,
            row("matches_result"))
        )
        .sortBy(_._1)
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5, name._6, name._7, name._8, name._9,
          name._10, name._11, name._12, name._13, name._14, name._15))
    matchesTuple.foreach(println)
  }

  //Cual es el top 10 de los maximos goleadores de la historia?
  def pregunta1(): ConnectionIO[List[(String, Int)]] = {
    sql"""
           SELECT CONCAT(p.given_name, ' ', p.family_name) AS Jugador, COUNT(DISTINCT g.goals_goal_id) AS Goles_anotados
    FROM players p
    JOIN goals g WHERE p.player_id = g.goals_player_id
    GROUP BY player_id
    ORDER BY Goles_anotados DESC
    LIMIT 10;
         """
      .query[(String, Int)]
      .to[List]
  }

  //Cual es el top 10 de los estadios en los que mas partidos se han jugado?
  def pregunta2(): ConnectionIO[List[(String, String, Int)]] = {
    sql"""
           SELECT s.stadium_name AS nombre_estadio, s.country_name AS pais,COUNT(DISTINCT m.matches_match_id) AS partidos_jugados
    FROM matches m
    JOIN stadiums s WHERE  m.matches_stadium_id = s.stadium_id
    GROUP BY s.stadium_id
    ORDER BY partidos_jugados DESC
    LIMIT 10;
         """
      .query[(String, String, Int)]
      .to[List]
  }

  //Cuantos goles en total ha anotado cada genero?
  def pregunta3(): ConnectionIO[List[(String, Int)]] = {
    sql"""
           SELECT CASE p.gender
            WHEN 0 THEN 'Masculino'
            WHEN 1 THEN 'Femenino'
            ELSE 'Otro'
    END AS genero , COUNT(DISTINCT g.goals_goal_id) AS Goles_totales
    FROM players p
    JOIN goals g WHERE p.player_id = g.goals_player_id
    GROUP BY p.gender;
         """
      .query[(String, Int)]
      .to[List]
  }

  //Cual es el top 10 de los anios en los que se jugaron mas partidos?
  def pregunta4(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
           SELECT t.tournaments_year AS años, COUNT(DISTINCT m.matches_match_id) AS partidos_jugados
    FROM tournaments t
    JOIN matches m on t.matches_tournament_id = m.matches_tournament_id
    GROUP BY t.tournaments_year
    ORDER BY partidos_jugados DESC
    LIMIT 10;
         """
      .query[(Int, Int)]
      .to[List]
  }

}
