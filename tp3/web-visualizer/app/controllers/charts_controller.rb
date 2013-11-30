class ChartsController < ApplicationController

  def index
    @airlines = FlownMiles.all.map { |data| [data.airline_code, data.airline_name] }
    @airlines.uniq!
  end

  def tweets_for
    render json: TweetCounter.all
  end

  def miles
    render json: FlownMiles.where(airline_code: params[:code]).order(:year)
  end
end