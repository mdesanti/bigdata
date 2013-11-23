class ChartsController < ApplicationController

  def index
  end

  def tweets_for
    render json: TweetCounter.where('lower(name) = ?', params[:name])
  end
end