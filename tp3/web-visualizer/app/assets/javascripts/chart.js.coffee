$ ->
  $(document).ready ->
    Highcharts.setOptions global:
      useUTC: false

    window.parties = ['pro', 'unen', 'otros', 'frente renovador', 'fpv']

    chart = undefined
    $("#tweets").highcharts

      chart:
        type: "spline"
        animation: Highcharts.svg # don't animate in old IE
        marginRight: 10
        events:
          load: ->
            
            series = this.series
            pie = this.series[this.series.length-1]
            setInterval (->
              $.ajax(
                url: "tweets"
              ).done (data) ->
                x = (new Date()).getTime() # current time
                $.each(data.charts, (index, value) ->
                  i = window.parties.indexOf(value.name.toLowerCase())
                  y = value.quantity
                  series[i].addPoint [x, y], true, false
                  pie.data[i].y = y
                  pie.isDirty = true
                  pie.redraw()
                )
            ), 5000

      title:
        text: "Cantidad de tweets por partido político"

      xAxis:
        type: "datetime"
        tickPixelInterval: 150

      yAxis:
        title:
          text: "Cantidad"

        plotLines: [
          value: 0
          width: 1
          color: "#808080"
        ]

      tooltip:
        formatter: ->
          "<b>" + @series.name + "</b><br/>" + Highcharts.dateFormat("%Y-%m-%d %H:%M:%S", @x) + "<br/>" + Highcharts.numberFormat(@y, 2)

      legend:
        enabled: true

      exporting:
        enabled: true

      series: [
        { name: 'pro', data: []},
        { name: 'unen', data: []},
        { name: 'otros', data: []},
        { name: 'frente renovador', data: []},
        { name: 'fpv', data: []},
        { 
          type: 'pie',
          name: 'Total consumption',
          data: [{
            name: 'Pro',
            y: 0,
            color: Highcharts.getOptions().colors[0] # Jane's color
          }, {
            name: 'Unen',
            y: 0,
            color: Highcharts.getOptions().colors[1] # John's color
          }, {
            name: 'Otros',
            y: 0,
            color: Highcharts.getOptions().colors[2] # Joe's color
          }, {
            name: 'Frente Renovador',
            y: 0,
            color: Highcharts.getOptions().colors[3] # Joe's color
          }, {
            name: 'FPV',
            y: 0,
            color: Highcharts.getOptions().colors[4] # Joe's color
          }],
          center: [100, 80],
          size: 150,
          dataLabels: {
            enabled: false
          }
        }
      ]

    $.each($('#airlines .data'), (index, value) ->
      code = $(value).attr('id')
      $.ajax(
        url: "miles?" + 'code=' + code
      ).done (data) ->
        console.log data
        airline = data.charts[0].airline_name
        years = getYears(data.charts)
        miles = getMiles(data.charts)
        create_chart(code, airline, years, miles)
    )

    tabs = $('#tabs li a')
    $(tabs).each ->
      $(window).resize()


getYears = (data) ->
  years = []
  $.each(data, (index, value) ->
    years.push(value.year)
  )
  return years

getMiles = (data) ->
  miles = []
  $.each(data, (index, value) ->
    miles.push(parseInt(value.miles))
  )  
  return miles

create_chart = (id, airline, years, miles) ->
  $("#chart-" + id).highcharts
    chart:
      type: "column"
      margin: [50, 50, 100, 80]

    title:
      text: "Flown miles for " + airline + " per year"

    xAxis:
      categories: years
      labels:
        rotation: -45
        align: "right"
        style:
          fontSize: "13px"
          fontFamily: "Verdana, sans-serif"

    yAxis:
      min: 0
      title:
        text: "Flown Miles"

    legend:
      enabled: false

    tooltip:
      pointFormat: "Flown miles: <b>{point.y:.1f}</b>"

    series: [
      name: "Flown Miles"
      data: miles
      dataLabels:
        enabled: true
        rotation: -90
        color: "#FFFFFF"
        align: "right"
        x: 4
        y: 10
        style:
          fontSize: "13px"
          fontFamily: "Verdana, sans-serif"
          textShadow: "0 0 3px black"
    ]
        