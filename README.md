Esempio di app creta con streamlit dedicata al gioco AFK Arena, dove usando un modello di classificazione Random Forest addestrato su una serie di incontri,
si va a prevedere l'esito dei match. Ogni giocatore possiede 5 eroi su una scelta totale di oltre un centinaio, perciò anche se attualmente riesce a funzionare discretamente 
servirebbe un dataset di dimensioni veramente grandi per migliorarlo ( la posizione del singolo eroe influenza l'esito, insieme al suo livello, perciò si dovrebbero gestire tutte
le combinazioni degli eroi per ciascuna delle 5 posizioni nel giocatore e avversarie, ottenendo una crescita esponenziale di osservazioni).
Nell'app il giocatore inserisce gli eroi della sua squadra per ogni posizione e il loro livello e lo stesso per la squadra avversaria, ottenendo infine la previsione dell'incontro.

link https://afkmatchforecaster.streamlit.app/
